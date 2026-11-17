"""
Run generated Wayang jobs and record row counts, run times and errors.

Compiles the jobs of a generated_jobs folder (TARGET_PLATFORM "Wayang") with sbt, which also downloads
Apache Wayang, and exports their classpath. Then it starts each JobXvY in its own JVM with
`java -cp <classpath> datafarm.generated.wayang.JobXvY <platforms> <data>` and records, per job, the
number of output rows, the run time the job measures itself (building, optimizing and executing the
Wayang plan, without JVM start-up), the wall-clock time of the whole JVM, and the error if it failed.

Usage:
    python datafarm/execution/wayang/run_wayang_jobs.py training-data/<EXPERIMENT_ID>/generated_jobs \\
        --data /path/to/data/ --output results.csv [--platforms java|spark|java,spark]

Needs Java 17 or later (Apache Wayang 1.1) and sbt. --platforms lists the platforms Wayang may choose
from; with several, Wayang's optimizer picks one per operator.
"""
import argparse
import csv
import glob
import os
import re
import subprocess
import time

PACKAGE = "datafarm.generated.wayang"
FIELDS = ["job", "platforms", "status", "rows", "ms", "wall_ms", "error"]
CLASSPATH_FILE = os.path.join("target", "datafarm-classpath.txt")

# JVM flags Spark (and so Wayang's Spark platform) needs on Java 17, as in Wayang's integration tests
JAVA17_FLAGS = [
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
] + ["--add-opens=java.base/%s=ALL-UNNAMED" % p for p in (
    "java.nio", "sun.nio.ch", "java.lang", "java.util", "java.io", "java.lang.reflect",
    "sun.reflect.annotation", "java.util.concurrent", "java.net", "java.lang.invoke")]


def job_names(folder):
    names = [os.path.splitext(os.path.basename(f))[0] for f in glob.glob(os.path.join(folder, "Job*v*.scala"))]
    names = [n for n in names if re.fullmatch(r"Job\d+v\d+", n)]
    return sorted(names, key=lambda n: [int(x) for x in re.findall(r"\d+", n)])


def build(folder, sbt="sbt", log=print):
    """Compiles the jobs with sbt and returns their runtime classpath (also saved to target/ for --no-build)."""
    log(f"Compiling the jobs in {folder} with sbt ...")
    p = subprocess.run([sbt, "-batch", "-Dsbt.color=false", "-Dsbt.supershell=false", "export Runtime/fullClasspath"],
                       cwd=folder, check=True, stdout=subprocess.PIPE, universal_newlines=True)
    cp = classpath_line(p.stdout)
    if not cp:
        lines = [l for l in p.stdout.splitlines() if l.strip()]
        raise SystemExit("Could not read the classpath from sbt's output:\n" + "\n".join(lines[-10:]))
    os.makedirs(os.path.join(folder, "target"), exist_ok=True)
    with open(os.path.join(folder, CLASSPATH_FILE), "w") as fh:
        fh.write(cp)
    return cp


ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def classpath_line(sbt_output):
    """The classpath sbt's `export` prints: the last line, without a log-level prefix, that lists the
    jobs' classes folder or jars (sbt may add colour codes and terminal control sequences around it)."""
    for line in reversed(sbt_output.splitlines()):
        line = ANSI.sub("", line).strip()
        if line and not line.startswith("[") and ("classes" in line or ".jar" in line):
            return line
    return None


def saved_classpath(folder):
    path = os.path.join(folder, CLASSPATH_FILE)
    if not os.path.exists(path):
        raise SystemExit(f"No {path}: run once without --no-build")
    with open(path) as fh:
        return fh.read().strip()


def default_java():
    home = os.environ.get("JAVA_HOME")
    return os.path.join(home, "bin", "java") if home else "java"


def error_line(stderr):
    """The most informative line of a failed job's stderr: the last exception, else its last line."""
    lines = [l.strip() for l in stderr.splitlines() if l.strip()]
    for l in reversed(lines):
        if re.search(r"(Exception|Error)\b[:\s]", l) and not l.startswith("at "):
            return l[:300]
    return lines[-1][:300] if lines else ""


def run(classpath, names, data, platforms="java", java="java", jvm_options=(), timeout=None, log=print):
    results = []
    for name in names:
        cmd = [java, *JAVA17_FLAGS, *jvm_options, "-cp", classpath, f"{PACKAGE}.{name}", platforms, data]
        start = time.time()
        try:
            p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                               timeout=timeout)
            wall_ms = round((time.time() - start) * 1000)
            m = re.search(rf"^{name}: (\d+) rows in (\d+) ms$", p.stdout, re.M)
            if p.returncode == 0 and m:
                r = {"job": name, "platforms": platforms, "status": "ok", "rows": int(m.group(1)),
                     "ms": int(m.group(2)), "wall_ms": wall_ms, "error": ""}
            else:
                err = error_line(p.stderr) or error_line(p.stdout) or \
                    f"exit code {p.returncode}, no result line in the output"
                r = {"job": name, "platforms": platforms, "status": "error", "rows": "", "ms": "",
                     "wall_ms": wall_ms, "error": err}
        except subprocess.TimeoutExpired:
            r = {"job": name, "platforms": platforms, "status": "timeout", "rows": "", "ms": "",
                 "wall_ms": round((time.time() - start) * 1000), "error": f"no result after {timeout} s"}
        results.append(r)
        log(f"| {name}: {r['status']} {r['rows']} {r['ms']} {r['error']}")
    return results


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folder", help="folder with the generated JobXvY.scala files and their build.sbt")
    ap.add_argument("--data", required=True, help="folder (or URL) with the input files, e.g. the TPC-H .tbl files")
    ap.add_argument("--platforms", default="java",
                    help="platforms Wayang may use: java, spark or java,spark (default: %(default)s)")
    ap.add_argument("--output", default="wayang_job_results.csv", help="results CSV (default: %(default)s)")
    ap.add_argument("--jobs", nargs="+", metavar="JobXvY", help="run only these jobs")
    ap.add_argument("--no-build", action="store_true",
                    help="use the classpath saved by the last build instead of compiling")
    ap.add_argument("--sbt", default="sbt", help="sbt command (default: %(default)s)")
    ap.add_argument("--java", default=default_java(), help="java command (default: $JAVA_HOME/bin/java, else java)")
    ap.add_argument("--jvm-option", action="append", default=[], metavar="OPTION",
                    help="extra JVM option, repeatable, e.g. --jvm-option=-Xmx8g")
    ap.add_argument("--timeout", type=int, default=0, help="timeout per job in seconds (0 = none)")
    args = ap.parse_args(argv)

    platforms = ",".join(p.strip().lower() for p in args.platforms.split(",") if p.strip())
    unknown = set(platforms.split(",")) - {"java", "spark"}
    if unknown or not platforms:
        raise SystemExit(f"--platforms must be java, spark or java,spark, not {args.platforms!r}")

    names = job_names(args.folder)
    if args.jobs:
        missing = sorted(set(args.jobs) - set(names))
        if missing:
            raise SystemExit(f"Not in {args.folder}: {', '.join(missing)}")
        names = [n for n in names if n in set(args.jobs)]
    if not names:
        raise SystemExit(f"No JobXvY.scala files in {args.folder}")

    classpath = saved_classpath(args.folder) if args.no_build else build(args.folder, args.sbt)
    data = args.data if ":/" in args.data else os.path.abspath(args.data)
    results = run(classpath, names, data, platforms, args.java, args.jvm_option, args.timeout or None)
    with open(args.output, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader(); w.writerows(results)
    errors = sum(r["status"] != "ok" for r in results)
    print(f"{len(results)} jobs, {errors} failed -> {args.output}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
