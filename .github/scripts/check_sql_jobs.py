"""CI check: create the tables of schema.sql in an empty DuckDB database and plan (EXPLAIN) every JobXvY.sql."""
import glob
import os
import sys

import duckdb

folder = sys.argv[1]
con = duckdb.connect()
con.execute(open(os.path.join(folder, "schema.sql"), encoding="utf-8").read())
jobs = sorted(glob.glob(os.path.join(folder, "Job*v*.sql")))
failed = []
for f in jobs:
    try:
        con.execute("EXPLAIN " + open(f, encoding="utf-8").read().strip().rstrip(";"))
    except Exception as e:
        failed.append(f"{os.path.basename(f)}: {str(e).splitlines()[0]}")
print(f"{len(jobs)} SQL jobs planned by DuckDB, {len(failed)} failed")
for line in failed:
    print("  " + line)
if not jobs or failed:
    sys.exit(1)
