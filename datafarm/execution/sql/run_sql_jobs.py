"""
Run generated SQL jobs on SQL Server and record row counts, run times and errors.

Works for both SQL Server outputs: TARGET_PLATFORM "SQLServer" (JobXvY.sql) and "SQLServerLTR"
(JobXvY.txt). With --showplan, it saves SQL Server's estimated plan (SET SHOWPLAN_XML ON) of each
job as JobXvY.xml instead of running it.

Usage:
    pip install pyodbc
    python datafarm/execution/sql/run_sql_jobs.py training-data/<EXPERIMENT_ID>/generated_jobs --output results.csv \\
        --connection "DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;DATABASE=...;UID=...;PWD=...;TrustServerCertificate=yes"
"""
import argparse
import csv
import glob
import os
import re
import time


def job_files(folder):
    files = glob.glob(os.path.join(folder, "Job*v*.sql")) + glob.glob(os.path.join(folder, "Job*v*.txt"))
    return sorted(files, key=lambda f: [int(x) for x in re.findall(r"\d+", os.path.basename(f))])


def run(conn, files, showplan_dir=None, log=print):
    results = []
    cur = conn.cursor()
    if showplan_dir:
        os.makedirs(showplan_dir, exist_ok=True)
        cur.execute("SET SHOWPLAN_XML ON")
    for f in files:
        name = os.path.splitext(os.path.basename(f))[0]
        sql = open(f, encoding="utf-8").read()
        start = time.time()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            ms = round((time.time() - start) * 1000)
            if showplan_dir:
                with open(os.path.join(showplan_dir, name + ".xml"), "w", encoding="utf-8") as out:
                    out.write(rows[0][0])
                results.append({"job": name, "status": "plan", "rows": "", "ms": ms, "error": ""})
            else:
                results.append({"job": name, "status": "ok", "rows": len(rows), "ms": ms, "error": ""})
        except Exception as e:  # keep going: one failing job should not stop the run
            results.append({"job": name, "status": "error", "rows": "", "ms": "", "error": str(e).splitlines()[0][:300]})
        log(f"| {name}: {results[-1]['status']} {results[-1]['rows']} {results[-1]['ms']} {results[-1]['error']}")
    if showplan_dir:
        cur.execute("SET SHOWPLAN_XML OFF")
    return results


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folder", help="folder with the generated JobXvY.sql or JobXvY.txt files")
    ap.add_argument("--connection", default=os.environ.get("DATAFARM_SQLSERVER"),
                    help="ODBC connection string (default: $DATAFARM_SQLSERVER)")
    ap.add_argument("--output", default="sql_job_results.csv")
    ap.add_argument("--showplan", metavar="DIR", help="save estimated XML plans to DIR instead of running the jobs")
    ap.add_argument("--timeout", type=int, default=0, help="query timeout in seconds (0 = none)")
    args = ap.parse_args(argv)
    if not args.connection:
        raise SystemExit("No connection string: pass --connection or set DATAFARM_SQLSERVER")

    import pyodbc
    conn = pyodbc.connect(args.connection, autocommit=True)
    conn.timeout = args.timeout
    results = run(conn, job_files(args.folder), args.showplan)
    with open(args.output, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["job", "status", "rows", "ms", "error"])
        w.writeheader(); w.writerows(results)
    errors = sum(r["status"] == "error" for r in results)
    print(f"{len(results)} jobs, {errors} errors -> {args.output}")


if __name__ == "__main__":
    main()
