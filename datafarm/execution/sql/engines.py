"""
The databases the SQL tools (load_sql_data.py, run_sql_jobs.py) work with, and what differs between them:
how to connect, how to bulk-load rows, and how to get a query's estimated plan.

| engine    | jobs (--platform)          | connection                                   | Python package      |
|-----------|----------------------------|----------------------------------------------|---------------------|
| duckdb    | SQL                        | database file (default: datafarm.duckdb)     | duckdb              |
| postgres  | SQL                        | libpq connection string or postgresql:// URL | psycopg (version 3) |
| sqlserver | SQLServer, SQLServerFlat   | ODBC connection string                       | pyodbc              |

The connection can also come from the environment variable DATAFARM_DUCKDB, DATAFARM_POSTGRES or
DATAFARM_SQLSERVER.
"""
import json
import os
import threading

ENGINES = ("duckdb", "postgres", "sqlserver")
ENV = {"duckdb": "DATAFARM_DUCKDB", "postgres": "DATAFARM_POSTGRES", "sqlserver": "DATAFARM_SQLSERVER"}
DEFAULT_CONNECTION = {"duckdb": "datafarm.duckdb"}
PACKAGE = {"duckdb": "duckdb", "postgres": "psycopg[binary]", "sqlserver": "pyodbc"}


def add_arguments(ap):
    ap.add_argument("--engine", required=True, choices=ENGINES, help="database to use")
    ap.add_argument("--connection",
                    help="duckdb: database file (default: datafarm.duckdb); postgres: connection string or URL; "
                         "sqlserver: ODBC connection string. Default: $DATAFARM_DUCKDB, $DATAFARM_POSTGRES or "
                         "$DATAFARM_SQLSERVER")


def connection_string(engine, value=None):
    value = value or os.environ.get(ENV[engine]) or DEFAULT_CONNECTION.get(engine)
    if not value:
        raise SystemExit(f"No connection for {engine}: pass --connection or set {ENV[engine]}")
    return value


def connect(engine, connection, autocommit=True):
    try:
        if engine == "duckdb":
            import duckdb
            return duckdb.connect(connection)
        if engine == "postgres":
            import psycopg
            return psycopg.connect(connection, autocommit=autocommit)
        import pyodbc
        return pyodbc.connect(connection, autocommit=autocommit)
    except ImportError:
        raise SystemExit(f"The {engine} engine needs the Python package {PACKAGE[engine]}: "
                         f"pip install '{PACKAGE[engine]}'")


def cursor(engine, conn):
    """A cursor; for DuckDB the connection itself (its cursor() is a separate connection, which
    Timeout's interrupt would not reach)."""
    return conn if engine == "duckdb" else conn.cursor()


def execute_script(engine, conn, sql):
    """Runs the statements of a script (e.g. schema.sql), skipping comment lines."""
    sql = "\n".join(l for l in sql.splitlines() if not l.lstrip().startswith("--"))
    cur = cursor(engine, conn)
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        cur.execute(stmt)
    conn.commit()


def insert_rows(engine, conn, table, rows, batch=10000, fast=True):
    """Bulk-loads rows (lists of strings, ints or None for NULL) into table; returns the row count.

    duckdb: batches of rows through a pandas DataFrame of strings (DuckDB casts them to the column types);
    postgres: COPY FROM STDIN; sqlserver: executemany with pyodbc's fast_executemany."""
    if engine == "postgres":
        n = 0
        with conn.cursor() as cur, cur.copy(f"COPY {table} FROM STDIN") as copy:
            for r in rows:
                copy.write_row(r)
                n += 1
        conn.commit()
        return n

    if engine == "duckdb":
        import pandas as pd

        def flush(buf):
            frame = pd.DataFrame([[None if v is None else str(v) for v in r] for r in buf], dtype=object,
                                 columns=[f"c{i}" for i in range(len(buf[0]))])
            conn.register("datafarm_rows", frame)
            conn.execute(f"INSERT INTO {table} SELECT * FROM datafarm_rows")
            conn.unregister("datafarm_rows")
    else:
        cur = conn.cursor()
        if fast:
            try:
                cur.fast_executemany = True  # pyodbc: much faster bulk inserts
            except AttributeError:
                pass
        sql = []

        def flush(buf):
            if not sql:
                sql.append(f"INSERT INTO {table} VALUES ({', '.join('?' * len(buf[0]))})")
            cur.executemany(sql[0], buf)
            conn.commit()

    n, buf = 0, []
    for r in rows:
        buf.append(r)
        if len(buf) >= batch:
            flush(buf); n += len(buf); buf = []
    if buf:
        flush(buf); n += len(buf)
    return n


def delete_rows(engine, conn, table):
    cursor(engine, conn).execute(f"DELETE FROM {table}")
    conn.commit()


class Timeout:
    """Stops a query after `seconds` (0 = never): SET statement_timeout in PostgreSQL, the connection
    timeout in SQL Server, and an interrupt from a timer thread in DuckDB."""

    def __init__(self, engine, conn, seconds):
        self.engine, self.conn, self.seconds, self.timer = engine, conn, seconds, None
        if seconds and engine == "postgres":
            conn.execute(f"SET statement_timeout = {int(seconds) * 1000}")
        elif seconds and engine == "sqlserver":
            conn.timeout = int(seconds)

    def __enter__(self):
        if self.seconds and self.engine == "duckdb":
            self.timer = threading.Timer(self.seconds, self.conn.interrupt)
            self.timer.start()
        return self

    def __exit__(self, *exc):
        if self.timer:
            self.timer.cancel()


def is_timeout(engine, error):
    text = str(error).lower()
    return ("interrupt" in text if engine == "duckdb" else
            "statement timeout" in text or "canceling statement" in text if engine == "postgres" else
            "timeout" in text)


class PlanMode:
    """Estimated plans instead of execution: EXPLAIN (FORMAT JSON) in DuckDB and PostgreSQL (saved as
    JobXvY.json), SET SHOWPLAN_XML ON in SQL Server (saved as JobXvY.xml)."""

    def __init__(self, engine, conn):
        self.engine, self.conn = engine, conn
        self.extension = "xml" if engine == "sqlserver" else "json"

    def __enter__(self):
        if self.engine == "sqlserver":
            self.conn.cursor().execute("SET SHOWPLAN_XML ON")  # applies to the session
        return self

    def __exit__(self, *exc):
        if self.engine == "sqlserver":
            self.conn.cursor().execute("SET SHOWPLAN_XML OFF")

    def plan(self, cur, sql):
        if self.engine == "sqlserver":
            cur.execute(sql)
            return cur.fetchall()[0][0]
        cur.execute("EXPLAIN (FORMAT JSON) " + sql)
        rows = cur.fetchall()
        # DuckDB returns (explain_key, plan) rows, PostgreSQL one row with the plan
        value = rows[-1][-1]
        return value if isinstance(value, str) else json.dumps(value, indent=1)
