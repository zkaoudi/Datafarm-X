"""
Load the data of a dataset into DuckDB, PostgreSQL or SQL Server for the generated SQL jobs.

The rows are parsed exactly as the generated Flink, Wayang and Spark jobs parse them: for a dataset
defined by a folder (datasets/<name>/, e.g. job or example) with the file format of its catalog.json
(datafarm/datasets/csvformat.py), for TPC-H as dbgen's '|'-separated .tbl files. So all platforms see the
same data; the engines' own CSV readers handle some files differently (BULK INSERT can not read the JOB
files at all), so this loader parses them itself.

Usage:
    python datafarm/execution/sql/load_sql_data.py --engine duckdb|postgres|sqlserver --dataset TPCH|<name> \\
        --data /path/to/data --schema training-data/<EXPERIMENT_ID>/generated_jobs/schema.sql [--connection ...]

--schema runs the CREATE TABLE statements first (the schema.sql written next to the generated SQL jobs:
--platform SQL for duckdb and postgres, SQLServer for sqlserver) and then loads only the tables it
creates, unless --tables is given; without --schema the tables must exist.
See engines.py for the connection of each engine.
"""
import argparse
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))  # repository root
from datafarm.execution.sql import engines  # noqa: E402
from datafarm.datasets import catalog as catalogs  # noqa: E402
from datafarm.datasets.csvformat import Format  # noqa: E402

TPCH_TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]


def tpch_rows(path):
    with open(path, encoding="utf-8", newline=None) as fh:
        for line in fh:
            line = line.rstrip("\n").rstrip("\r")
            fields = line.split("|")
            if fields and fields[-1] == "":  # dbgen ends every line with '|'
                fields = fields[:-1]
            yield fields  # the database converts the strings to INT/DECIMAL/DATE


def load(engine, conn, dataset, data_dir, tables=None, batch=10000, fast=True, truncate=False, log=print):
    """Loads the files of `dataset` from `data_dir` into the existing tables; returns the row counts."""
    if dataset.upper() == "TPCH":
        tables = tables or [t for t in TPCH_TABLES if os.path.exists(os.path.join(data_dir, t + ".tbl"))]
        files = {t: os.path.join(data_dir, t + ".tbl") for t in tables}
        rows_of = lambda t: tpch_rows(files[t])
    else:
        cat = catalogs.load(dataset)
        fmt = Format.from_json(cat["format"])
        by_name = {t["name"]: t for t in cat["tables"]}
        tables = tables or [t["name"] for t in cat["tables"]]
        unknown = [t for t in tables if t not in by_name]
        if unknown:
            raise SystemExit(f"Dataset {cat['name']} has no table {', '.join(unknown)}")
        files = {t: os.path.join(data_dir, by_name[t]["file"]) for t in tables}
        rows_of = lambda t: fmt.rows(files[t], [(c["name"], c["type"] == "int") for c in by_name[t]["columns"]])
    if not tables:
        raise SystemExit(f"No {dataset} files in {data_dir}")
    missing = [f for f in files.values() if not os.path.exists(f)]
    if missing:
        raise SystemExit(f"Missing files: {', '.join(missing)}")

    counts = {}
    for t in tables:
        if truncate:
            engines.delete_rows(engine, conn, t)
        rows = rows_of(t)
        start = time.time()
        counts[t] = engines.insert_rows(engine, conn, t, rows, batch=batch, fast=fast)
        log(f"| {t}: {counts[t]:,} rows in {time.time() - start:.1f} s")
    return counts


def created_tables(script):
    """Names of the tables a script creates, in order (comment lines skipped)."""
    sql = "\n".join(l for l in script.splitlines() if not l.lstrip().startswith("--"))
    return [m.lower() for m in re.findall(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][\w.]*)", sql, re.I)]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    engines.add_arguments(ap)
    ap.add_argument("--dataset", required=True,
                    help="TPCH, or a dataset defined by a folder: its name in datasets/ (e.g. job) or its path")
    ap.add_argument("--data", required=True, help="folder with the data files")
    ap.add_argument("--schema", help="schema.sql to run first (CREATE TABLE); without --tables, only the tables it creates are loaded")
    ap.add_argument("--tables", help="comma-separated subset of tables")
    ap.add_argument("--batch", type=int, default=10000, help="rows per insert batch (duckdb, sqlserver)")
    ap.add_argument("--truncate", action="store_true", help="delete existing rows first")
    ap.add_argument("--no-fast-executemany", action="store_true",
                    help="sqlserver: use plain executemany (slower; try it if the driver fails on NVARCHAR(MAX) columns)")
    args = ap.parse_args(argv)

    conn = engines.connect(args.engine, engines.connection_string(args.engine, args.connection),
                           autocommit=args.engine != "sqlserver")
    tables = args.tables.split(",") if args.tables else None
    if args.schema:
        script = open(args.schema, encoding="utf-8").read()
        engines.execute_script(args.engine, conn, script)
        # the schema.sql of generated jobs only creates the tables they use: load just those
        tables = tables or created_tables(script)
    load(args.engine, conn, args.dataset, args.data, tables,
         batch=args.batch, fast=not args.no_fast_executemany, truncate=args.truncate)
    conn.close()


if __name__ == "__main__":
    main()
