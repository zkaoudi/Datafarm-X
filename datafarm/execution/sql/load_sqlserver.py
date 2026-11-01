"""
Load TPC-H or JOB (IMDB) data into SQL Server for the generated SQL jobs.

The rows are parsed exactly as the generated Wayang/Spark jobs parse them (for JOB with the same CSV
rules as datafarm/datasets_metadata/job/MetaDataAnalyzer.py: backslash escapes, empty
unquoted field = NULL), so all platforms see the same data. BULK INSERT cannot read the
backslash-escaped JOB files; for TPC-H, BULK INSERT (see the header of schema.sql) is faster.

Usage:
    pip install pyodbc
    python datafarm/execution/sql/load_sqlserver.py --dataset JOB --data /path/to/imdb_csv \\
        --connection "DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;DATABASE=imdb;UID=...;PWD=...;TrustServerCertificate=yes" \\
        --schema training-data/<EXPERIMENT_ID>/generated_jobs/schema.sql

--schema runs the CREATE TABLE statements first (the schema.sql written next to SQLServer jobs);
without it the tables must exist. Credentials can also come from the DATAFARM_SQLSERVER environment
variable instead of --connection.
"""
import argparse
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))  # repository root
from datafarm.datasets_metadata.job import MetaDataAnalyzer as JOB  # noqa: E402  (CSV parsing and column types of the JOB tables)

TPCH_TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]


def tpch_rows(path):
    with open(path, encoding="utf-8", newline=None) as fh:
        for line in fh:
            line = line.rstrip("\n").rstrip("\r")
            fields = line.split("|")
            if fields and fields[-1] == "":  # dbgen ends every line with '|'
                fields = fields[:-1]
            yield fields  # SQL Server converts the strings to INT/DECIMAL/DATE


def job_rows(path, table):
    cols = JOB.SCHEMA[table]
    with open(path, encoding="utf-8", errors="replace", newline=None) as fh:
        for line in fh:
            if line.endswith("\n"):
                line = line[:-1]
            raw = JOB.split(line, len(cols))
            yield [(None if JOB.parse_int(v) == JOB.NULL_INT else JOB.parse_int(v)) if is_int else v
                   for v, (_, is_int, _) in zip(raw, cols)]


def create_tables(conn, schema_path):
    ddl = open(schema_path, encoding="utf-8").read()
    ddl = "\n".join(l for l in ddl.splitlines() if not l.lstrip().startswith("--"))
    cur = conn.cursor()
    for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
        cur.execute(stmt)
    conn.commit()


def load(conn, dataset, data_dir, tables=None, batch=10000, fast=True, truncate=False, log=print):
    """Insert the files of `dataset` from `data_dir` through a DB-API connection with '?' parameters."""
    if dataset == "JOB":
        tables = tables or sorted(JOB.SCHEMA)
        files = {t: os.path.join(data_dir, t + ".csv") for t in tables}
    else:
        tables = tables or [t for t in TPCH_TABLES if os.path.exists(os.path.join(data_dir, t + ".tbl"))]
        files = {t: os.path.join(data_dir, t + ".tbl") for t in tables}
    missing = [f for f in files.values() if not os.path.exists(f)]
    if missing:
        raise SystemExit(f"Missing files: {', '.join(missing)}")

    counts = {}
    for t in tables:
        cur = conn.cursor()
        if fast:
            try:
                cur.fast_executemany = True  # pyodbc: much faster bulk inserts
            except AttributeError:
                pass
        if truncate:
            cur.execute(f"DELETE FROM {t}")
        rows = job_rows(files[t], t) if dataset == "JOB" else tpch_rows(files[t])
        n, buf, start, sql = 0, [], time.time(), None
        for r in rows:
            if sql is None:
                sql = f"INSERT INTO {t} VALUES ({', '.join('?' * len(r))})"
            buf.append(r)
            if len(buf) >= batch:
                cur.executemany(sql, buf); conn.commit(); n += len(buf); buf = []
        if buf:
            cur.executemany(sql, buf); conn.commit(); n += len(buf)
        counts[t] = n
        log(f"| {t}: {n:,} rows in {time.time() - start:.1f} s")
    return counts


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", required=True, choices=["JOB", "TPCH"])
    ap.add_argument("--data", required=True, help="folder with the .csv (JOB) or .tbl (TPC-H) files")
    ap.add_argument("--connection", default=os.environ.get("DATAFARM_SQLSERVER"),
                    help="ODBC connection string (default: $DATAFARM_SQLSERVER)")
    ap.add_argument("--schema", help="schema.sql to run first (CREATE TABLE)")
    ap.add_argument("--tables", help="comma-separated subset of tables")
    ap.add_argument("--batch", type=int, default=10000)
    ap.add_argument("--truncate", action="store_true", help="delete existing rows first")
    ap.add_argument("--no-fast-executemany", action="store_true",
                    help="use plain executemany (slower; try it if the driver fails on NVARCHAR(MAX) columns)")
    args = ap.parse_args(argv)
    if not args.connection:
        raise SystemExit("No connection string: pass --connection or set DATAFARM_SQLSERVER")

    import pyodbc
    conn = pyodbc.connect(args.connection, autocommit=False)
    if args.schema:
        create_tables(conn, args.schema)
    load(conn, args.dataset, args.data, args.tables.split(",") if args.tables else None,
         batch=args.batch, fast=not args.no_fast_executemany, truncate=args.truncate)


if __name__ == "__main__":
    main()
