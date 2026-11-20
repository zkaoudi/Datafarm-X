"""
Run generated Flink jobs locally and record row counts, run times and errors.

The generator writes each Flink job as its own sbt project (generated_jobs/JobXvY/). This script
compiles all of them at once in one sbt project (target/flink-local/ in the jobs folder, with Flink as a
compile dependency) and exports its classpath. Then it starts each JobXvY in its own JVM with Flink's
local execution environment and records, per job, the number of output rows and the net run time Flink
reports, the wall-clock time of the whole JVM, and the error if it failed.

Row counts are reported by the jobs of datasets defined by a catalog (datasets/<name>/), whose sink
counts its rows; TPC-H jobs only report their run time (rows stays empty).

Usage:
    python datafarm/execution/flink/run_flink_jobs.py training-data/<EXPERIMENT_ID>/generated_jobs \\
        --data /path/to/data/ --output results.csv [--parallelism 2]

Needs Java 8 or 11 (Flink 1.10) and sbt. To submit jobs to a Flink cluster instead, build each project
with `sbt assembly` and use datafarm/execution/flink/BuildAndSubmit.py.
"""
import argparse
import csv
import glob
import os
import re
import shutil
import subprocess
import tempfile
import time

FIELDS = ["job", "status", "rows", "ms", "wall_ms", "error"]
BUILD_DIR = os.path.join("target", "flink-local")
CLASSPATH_FILE = "datafarm-classpath.txt"
FLINK_VERSION = "1.10.0"

BUILD_SBT = f"""name := "datafarm-flink-jobs"

version := "0.1"

scalaVersion := "2.11.12"

val flinkVersion = "{FLINK_VERSION}"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "joda-time" % "joda-time" % "2.8.1"
)
"""


def job_names(folder):
    names = [os.path.basename(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(f)))))
             for f in glob.glob(os.path.join(folder, "Job*v*", "src", "main", "scala", "Job*v*.scala"))]
    names = [n for n in names if re.fullmatch(r"Job\d+v\d+", n)]
    return sorted(names, key=lambda n: [int(x) for x in re.findall(r"\d+", n)])


ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def classpath_line(sbt_output):
    """The classpath sbt's `export` prints: the last line without a log-level prefix that lists jars or classes."""
    for line in reversed(sbt_output.splitlines()):
        line = ANSI.sub("", line).strip()
        if line and not line.startswith("[") and ("classes" in line or ".jar" in line):
            return line
    return None


def build(folder, names, sbt="sbt", log=print):
    """Copies the jobs into one sbt project, compiles it and returns its runtime classpath."""
    project = os.path.join(folder, BUILD_DIR)
    src = os.path.join(project, "src", "main", "scala")
    if os.path.isdir(src):
        shutil.rmtree(src)
    os.makedirs(src)
    os.makedirs(os.path.join(project, "project"), exist_ok=True)
    for n in names:
        shutil.copy(os.path.join(folder, n, "src", "main", "scala", n + ".scala"), src)
    with open(os.path.join(project, "build.sbt"), "w") as fh:
        fh.write(BUILD_SBT)
    with open(os.path.join(project, "project", "build.properties"), "w") as fh:
        fh.write("sbt.version=1.9.9\n")
    log(f"Compiling {len(names)} Flink jobs in {project} with sbt ...")
    p = subprocess.run([sbt, "-batch", "-Dsbt.color=false", "-Dsbt.supershell=false", "export Runtime/fullClasspath"],
                       cwd=project, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    if p.returncode != 0:
        errors = [ANSI.sub("", l) for l in p.stdout.splitlines() if "[error]" in l]
        raise SystemExit("sbt failed to compile the Flink jobs:\n" + "\n".join((errors or p.stdout.splitlines())[-60:]))
    cp = classpath_line(p.stdout)
    if not cp:
        lines = [l for l in p.stdout.splitlines() if l.strip()]
        raise SystemExit("Could not read the classpath from sbt's output:\n" + "\n".join(lines[-10:]))
    with open(os.path.join(project, CLASSPATH_FILE), "w") as fh:
        fh.write(cp)
    return cp


def saved_classpath(folder):
    path = os.path.join(folder, BUILD_DIR, CLASSPATH_FILE)
    if not os.path.exists(path):
        raise SystemExit(f"No {path}: run once without --no-build")
    with open(path) as fh:
        return fh.read().strip()


def default_java():
    home = os.environ.get("JAVA_HOME")
    return os.path.join(home, "bin", "java") if home else "java"


def error_line(stderr):
    """The most informative line of a failed job's output: the last exception, else its last line."""
    lines = [l.strip() for l in stderr.splitlines() if l.strip()]
    for l in reversed(lines):
        if re.search(r"(Exception|Error)\b[:\s]", l) and not l.startswith("at "):
            return l[:300]
    return lines[-1][:300] if lines else ""


def run(classpath, names, data, plan_dir, parallelism=2, heap="1g", java="java", jvm_options=(), timeout=None, log=print):
    results = []
    for name in names:
        cmd = [java, *jvm_options, "-cp", classpath, name, data, plan_dir, "exec", "local", heap, str(parallelism)]
        start = time.time()
        try:
            p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                               timeout=timeout)
            wall_ms = round((time.time() - start) * 1000)
            m = re.search(rf"^{name}: (\d+) rows in (\d+) ms$", p.stdout, re.M)
            t = re.search(r"^Runtime execution: (\d+) ms$", p.stdout, re.M)
            if p.returncode == 0 and (m or t):
                r = {"job": name, "status": "ok", "rows": int(m.group(1)) if m else "",
                     "ms": int(m.group(2)) if m else int(t.group(1)), "wall_ms": wall_ms, "error": ""}
            else:
                err = error_line(p.stderr) or error_line(p.stdout) or \
                    f"exit code {p.returncode}, no result line in the output"
                r = {"job": name, "status": "error", "rows": "", "ms": "", "wall_ms": wall_ms, "error": err}
        except subprocess.TimeoutExpired:
            r = {"job": name, "status": "timeout", "rows": "", "ms": "",
                 "wall_ms": round((time.time() - start) * 1000), "error": f"no result after {timeout} s"}
        results.append(r)
        log(f"| {name}: {r['status']} {r['rows']} {r['ms']} {r['error']}")
    return results


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folder", help="generated_jobs folder with one JobXvY/ sbt project per job")
    ap.add_argument("--data", help="folder with the input files (required unless --build-only)")
    ap.add_argument("--output", default="flink_job_results.csv", help="results CSV (default: %(default)s)")
    ap.add_argument("--plans", metavar="DIR", help="folder for the execution plans the jobs save (default: a temporary one)")
    ap.add_argument("--jobs", nargs="+", metavar="JobXvY", help="run only these jobs")
    ap.add_argument("--parallelism", type=int, default=2, help="parallelism of the local environment (default: %(default)s)")
    ap.add_argument("--heap", default="1g", help="task manager heap of the local environment (default: %(default)s)")
    ap.add_argument("--no-build", action="store_true", help="use the classpath saved by the last build")
    ap.add_argument("--build-only", action="store_true", help="only compile the jobs")
    ap.add_argument("--sbt", default="sbt", help="sbt command (default: %(default)s)")
    ap.add_argument("--java", default=default_java(), help="java command (default: $JAVA_HOME/bin/java, else java)")
    ap.add_argument("--jvm-option", action="append", default=[], metavar="OPTION",
                    help="extra JVM option, repeatable, e.g. --jvm-option=-Xmx4g")
    ap.add_argument("--timeout", type=int, default=0, help="timeout per job in seconds (0 = none)")
    args = ap.parse_args(argv)

    names = job_names(args.folder)
    if args.jobs:
        missing = sorted(set(args.jobs) - set(names))
        if missing:
            raise SystemExit(f"Not in {args.folder}: {', '.join(missing)}")
        names = [n for n in names if n in set(args.jobs)]
    if not names:
        raise SystemExit(f"No JobXvY/ Flink projects in {args.folder}")

    classpath = saved_classpath(args.folder) if args.no_build else build(args.folder, job_names(args.folder), args.sbt)
    if args.build_only:
        print(f"{len(job_names(args.folder))} Flink jobs compiled")
        return 0
    if not args.data:
        ap.error("--data is required")
    data = os.path.join(os.path.abspath(args.data), "")  # the jobs append the file names
    plan_dir = os.path.abspath(args.plans) if args.plans else tempfile.mkdtemp(prefix="flink-plans-")
    os.makedirs(plan_dir, exist_ok=True)
    results = run(classpath, names, data, plan_dir, args.parallelism, args.heap, args.java, args.jvm_option,
                  args.timeout or None)
    with open(args.output, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader(); w.writerows(results)
    errors = sum(r["status"] != "ok" for r in results)
    print(f"{len(results)} jobs, {errors} failed -> {args.output}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
