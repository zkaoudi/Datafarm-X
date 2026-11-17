"""
Run generated Spark jobs with spark-submit and record row counts, run times and errors.

Builds the jobs of a generated_jobs folder (TARGET_PLATFORM "Spark") with `sbt package`, then submits
each JobXvY with spark-submit and records, per job, the number of output rows, the net run time the
job measures itself (the run time of its count() action, without start-up), the wall-clock time of
the whole spark-submit, and the error if it failed. With --plans, each job also writes JobXvY.json
with its run time, row count and RDD lineage there.

Usage:
    python datafarm/execution/spark/run_spark_jobs.py training-data/<EXPERIMENT_ID>/generated_jobs \\
        --data /path/to/data/ --output results.csv [--master local[*]] [--plans DIR]

spark-submit is taken from $SPARK_HOME/bin, or from the PATH (e.g. `pip install pyspark==3.5.3`).
The jobs are built for Spark 3.5 and Scala 2.12, so use a Spark 3.5 build for Scala 2.12.
"""
import argparse
import csv
import glob
import os
import re
import subprocess
import time

PACKAGE = "datafarm.generated.spark"
FIELDS = ["job", "status", "rows", "ms", "wall_ms", "error"]


def job_names(folder):
    names = [os.path.splitext(os.path.basename(f))[0] for f in glob.glob(os.path.join(folder, "Job*v*.scala"))]
    names = [n for n in names if re.fullmatch(r"Job\d+v\d+", n)]
    return sorted(names, key=lambda n: [int(x) for x in re.findall(r"\d+", n)])


def build(folder, sbt="sbt", log=print):
    """Runs `sbt package` in the jobs folder and returns the path of the jar."""
    log(f"Building the jobs in {folder} with sbt package ...")
    subprocess.run([sbt, "-batch", "package"], cwd=folder, check=True)
    return find_jar(folder)


def find_jar(folder):
    jars = [j for j in glob.glob(os.path.join(folder, "target", "scala-*", "*.jar"))
            if not j.endswith(("-sources.jar", "-javadoc.jar"))]
    if not jars:
        raise SystemExit(f"No jar in {folder}/target/scala-*/: build the jobs with `sbt package` first")
    return max(jars, key=os.path.getmtime)


def default_spark_submit():
    home = os.environ.get("SPARK_HOME")
    return os.path.join(home, "bin", "spark-submit") if home else "spark-submit"


def data_path(path):
    """Spark reads `datapath + "<table>.tbl"`, so the path must end with '/'; local paths are made absolute."""
    if "://" not in path and not path.startswith("file:"):
        path = os.path.abspath(path)
    return path if path.endswith("/") else path + "/"


def error_line(stderr):
    """The most informative line of a failed job's stderr: the last exception, else its last line."""
    lines = [l.strip() for l in stderr.splitlines() if l.strip()]
    for l in reversed(lines):
        if re.search(r"(Exception|Error)\b[:\s]", l) and not l.startswith("at "):
            return l[:300]
    return lines[-1][:300] if lines else ""


def run(jar, names, data, spark_submit="spark-submit", master="local[*]", plans_dir=None, spark_args=(),
        timeout=None, log=print):
    results = []
    if plans_dir:
        os.makedirs(plans_dir, exist_ok=True)
    for name in names:
        cmd = [spark_submit, "--class", f"{PACKAGE}.{name}", "--master", master, *spark_args, jar, data]
        if plans_dir:
            cmd.append(os.path.abspath(plans_dir))
        start = time.time()
        try:
            p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                               timeout=timeout)
            wall_ms = round((time.time() - start) * 1000)
            m = re.search(rf"^{name}: (\d+) rows in (\d+) ms$", p.stdout, re.M)
            if p.returncode == 0 and m:
                r = {"job": name, "status": "ok", "rows": int(m.group(1)), "ms": int(m.group(2)),
                     "wall_ms": wall_ms, "error": ""}
            else:
                err = error_line(p.stderr) or f"exit code {p.returncode}, no result line in the output"
                r = {"job": name, "status": "error", "rows": "", "ms": "", "wall_ms": wall_ms, "error": err}
        except subprocess.TimeoutExpired:
            r = {"job": name, "status": "timeout", "rows": "", "ms": "", "wall_ms": round((time.time() - start) * 1000),
                 "error": f"no result after {timeout} s"}
        results.append(r)
        log(f"| {name}: {r['status']} {r['rows']} {r['ms']} {r['error']}")
    return results


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folder", help="folder with the generated JobXvY.scala files and their build.sbt")
    ap.add_argument("--data", required=True, help="folder (or URL) with the input files, e.g. the TPC-H .tbl files")
    ap.add_argument("--output", default="spark_job_results.csv", help="results CSV (default: %(default)s)")
    ap.add_argument("--master", default="local[*]", help="Spark master (default: %(default)s)")
    ap.add_argument("--plans", metavar="DIR", help="let each job write JobXvY.json with its RDD lineage to DIR")
    ap.add_argument("--jobs", nargs="+", metavar="JobXvY", help="run only these jobs")
    ap.add_argument("--jar", help="use this jar instead of building the jobs with sbt package")
    ap.add_argument("--no-build", action="store_true", help="use the jar already in target/ instead of building")
    ap.add_argument("--sbt", default="sbt", help="sbt command (default: %(default)s)")
    ap.add_argument("--spark-submit", default=default_spark_submit(),
                    help="spark-submit command (default: $SPARK_HOME/bin/spark-submit, else spark-submit)")
    ap.add_argument("--conf", action="append", default=[], metavar="KEY=VALUE",
                    help="Spark configuration passed to spark-submit, repeatable")
    ap.add_argument("--driver-memory", help="spark-submit --driver-memory, e.g. 4g")
    ap.add_argument("--timeout", type=int, default=0, help="timeout per job in seconds (0 = none)")
    args = ap.parse_args(argv)

    names = job_names(args.folder)
    if args.jobs:
        missing = sorted(set(args.jobs) - set(names))
        if missing:
            raise SystemExit(f"Not in {args.folder}: {', '.join(missing)}")
        names = [n for n in names if n in set(args.jobs)]
    if not names:
        raise SystemExit(f"No JobXvY.scala files in {args.folder}")

    jar = args.jar or (find_jar(args.folder) if args.no_build else build(args.folder, args.sbt))
    spark_args = [a for kv in args.conf for a in ("--conf", kv)]
    if args.driver_memory:
        spark_args += ["--driver-memory", args.driver_memory]

    results = run(jar, names, data_path(args.data), args.spark_submit, args.master, args.plans, spark_args,
                  args.timeout or None)
    with open(args.output, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader(); w.writerows(results)
    errors = sum(r["status"] != "ok" for r in results)
    print(f"{len(results)} jobs, {errors} failed -> {args.output}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
