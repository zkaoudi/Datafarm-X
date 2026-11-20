"""
Run generated SQL jobs on DuckDB, PostgreSQL or SQL Server and record row counts, run times and errors.

Runs every JobXvY.sql (--platform SQL or SQLServer) or JobXvY.txt (--platform SQLServerFlat) of a folder
and writes a CSV with, per job, the status (ok, error or timeout), the number of rows of its result, the
run time (executing the query and fetching its result), and the error if it failed. With --plans DIR it
saves each job's estimated plan instead of running it: EXPLAIN (FORMAT JSON) in DuckDB and PostgreSQL
(JobXvY.json), SHOWPLAN_XML in SQL Server (JobXvY.xml).

Usage:
    python datafarm/execution/sql/run_sql_jobs.py training-data/<EXPERIMENT_ID>/generated_jobs \\
        --engine duckdb|postgres|sqlserver [--connection ...] --output results.csv [--plans DIR] [--timeout 600]

Load the data first with load_sql_data.py. See engines.py for the connection of each engine.
"""
import argparse
import csv
import glob
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))  # repository root
from datafarm.execution.sql import engines  # noqa: E402

FIELDS = ["job", "engine", "status", "rows", "ms", "error"]


def job_files(folder):
    files = glob.glob(os.path.join(folder, "Job*v*.sql")) + glob.glob(os.path.join(folder, "Job*v*.txt"))
    files = [f for f in files if re.fullmatch(r"Job\d+v\d+", os.path.splitext(os.path.basename(f))[0])]
    return sorted(files, key=lambda f: [int(x) for x in re.findall(r"\d+", os.path.basename(f))])


def count_rows(cur, batch=10000):
    n = 0
    while True:
        rows = cur.fetchmany(batch)
        if not rows:
            return n
        n += len(rows)


def run(engine, conn, files, plans_dir=None, timeout=0, log=print):
    results = []
    cur = engines.cursor(engine, conn)
    timer = engines.Timeout(engine, conn, timeout)
    plan_mode = engines.PlanMode(engine, conn) if plans_dir else None
    if plans_dir:
        os.makedirs(plans_dir, exist_ok=True)
        plan_mode.__enter__()
    try:
        for f in files:
            name = os.path.splitext(os.path.basename(f))[0]
            sql = open(f, encoding="utf-8").read().strip().rstrip(";")
            r = {"job": name, "engine": engine, "status": "ok", "rows": "", "ms": "", "error": ""}
            start = time.time()
            try:
                with timer:
                    if plan_mode:
                        plan = plan_mode.plan(cur, sql)
                        with open(os.path.join(plans_dir, f"{name}.{plan_mode.extension}"), "w", encoding="utf-8") as out:
                            out.write(plan)
                        r["status"] = "plan"
                    else:
                        cur.execute(sql)
                        r["rows"] = count_rows(cur)
                r["ms"] = round((time.time() - start) * 1000)
            except Exception as e:  # keep going: one failing job should not stop the run
                r["status"] = "timeout" if timeout and engines.is_timeout(engine, e) else "error"
                r["error"] = str(e).strip().splitlines()[0][:300] if str(e).strip() else type(e).__name__
                if engine == "postgres":
                    conn.rollback()
            results.append(r)
            log(f"| {name}: {r['status']} {r['rows']} {r['ms']} {r['error']}")
    finally:
        if plan_mode:
            plan_mode.__exit__(None, None, None)
    return results


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folder", help="folder with the generated JobXvY.sql or JobXvY.txt files")
    engines.add_arguments(ap)
    ap.add_argument("--output", default="sql_job_results.csv", help="results CSV (default: %(default)s)")
    ap.add_argument("--plans", "--showplan", metavar="DIR", dest="plans",
                    help="save the estimated plans to DIR instead of running the jobs")
    ap.add_argument("--jobs", nargs="+", metavar="JobXvY", help="run only these jobs")
    ap.add_argument("--timeout", type=int, default=0, help="timeout per job in seconds (0 = none)")
    args = ap.parse_args(argv)

    files = job_files(args.folder)
    if args.jobs:
        names = {os.path.splitext(os.path.basename(f))[0] for f in files}
        missing = sorted(set(args.jobs) - names)
        if missing:
            raise SystemExit(f"Not in {args.folder}: {', '.join(missing)}")
        files = [f for f in files if os.path.splitext(os.path.basename(f))[0] in set(args.jobs)]
    if not files:
        raise SystemExit(f"No JobXvY.sql or JobXvY.txt files in {args.folder}")

    conn = engines.connect(args.engine, engines.connection_string(args.engine, args.connection))
    results = run(args.engine, conn, files, args.plans, args.timeout)
    conn.close()
    with open(args.output, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader(); w.writerows(results)
    errors = sum(r["status"] in ("error", "timeout") for r in results)
    print(f"{len(results)} jobs, {errors} failed -> {args.output}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
