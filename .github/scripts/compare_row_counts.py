"""CI check: every generated job returns the same number of rows on every platform.

Takes the results CSVs of the runners (columns job, status, rows) as LABEL=PATH, and optionally runs the
generated SQL jobs in DuckDB on the same data (--sql-jobs with --data, the folder of dbgen .tbl files).
Fails if a job failed anywhere or if its row counts differ.

    python .github/scripts/compare_row_counts.py spark=spark.csv wayang-java=wayang_java.csv \
        --sql-jobs training-data/SQL_TPCH/generated_jobs --data /path/to/tpch
"""
import argparse
import csv
import glob
import os
import re
import sys
import tempfile

import duckdb


def read_results(path):
    with open(path, newline="") as fh:
        return {r["job"]: (int(r["rows"]) if r["status"] == "ok" else r["status"]) for r in csv.DictReader(fh)}


def duckdb_rows(sql_folder, data):
    """Loads the .tbl files into the tables of schema.sql and counts the rows of every JobXvY.sql."""
    con = duckdb.connect()
    con.execute(open(os.path.join(sql_folder, "schema.sql"), encoding="utf-8").read())
    tables = [r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()]
    with tempfile.TemporaryDirectory() as tmp:
        for t in tables:
            # dbgen ends every line with '|'
            src, dst = os.path.join(data, t + ".tbl"), os.path.join(tmp, t + ".tbl")
            with open(src, encoding="utf-8") as fin, open(dst, "w", encoding="utf-8") as fout:
                for line in fin:
                    fout.write(line.rstrip("\n").rstrip("|") + "\n")
            con.execute(f"COPY {t} FROM '{dst}' (DELIMITER '|', HEADER false)")
    rows = {}
    for f in glob.glob(os.path.join(sql_folder, "Job*v*.sql")):
        job = os.path.splitext(os.path.basename(f))[0]
        query = open(f, encoding="utf-8").read().strip().rstrip(";")
        try:
            rows[job] = con.execute(f"SELECT COUNT(*) FROM (\n{query}\n) q").fetchone()[0]
        except Exception as e:
            rows[job] = "error: " + str(e).splitlines()[0]
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", nargs="+", metavar="LABEL=CSV", help="results CSV of a runner, with a label")
    ap.add_argument("--sql-jobs", help="folder with the generated SQL jobs and their schema.sql")
    ap.add_argument("--data", help="folder with the dbgen .tbl files (with --sql-jobs)")
    args = ap.parse_args()

    platforms = {}
    for item in args.results:
        label, _, path = item.partition("=")
        platforms[label] = read_results(path)
    if args.sql_jobs:
        platforms["sql-duckdb"] = duckdb_rows(args.sql_jobs, args.data)

    labels = list(platforms)
    jobs = sorted(set().union(*platforms.values()), key=lambda n: [int(x) for x in re.findall(r"\d+", n)])
    print("job      " + "".join(f"{l:>14}" for l in labels))
    bad = []
    for job in jobs:
        values = [platforms[l].get(job, "missing") for l in labels]
        print(f"{job:<9}" + "".join(f"{str(v):>14}" for v in values))
        if any(not isinstance(v, int) for v in values) or len(set(values)) > 1:
            bad.append(job)
    if bad:
        print(f"{len(bad)} of {len(jobs)} jobs differ between platforms or failed: {', '.join(bad)}")
        sys.exit(1)
    print(f"{len(jobs)} jobs return the same number of rows on {', '.join(labels)}")


if __name__ == "__main__":
    main()
