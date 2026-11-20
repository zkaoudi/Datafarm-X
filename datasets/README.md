# Datasets

Each folder here is a dataset DataFarm can generate jobs for, on every platform: Flink, Wayang, Spark, SQL and
SQL Server. To add your own dataset, give DataFarm its schema, a few seed queries and the data. You write no code.

| Folder | Dataset |
|---|---|
| [`example/`](example/) | A small synthetic web shop, with data. It shows how to add a dataset and runs in CI on all platforms. |
| [`job/`](job/) | The IMDB database of the Join Order Benchmark (JOB). The data and the 113 queries are not included; see its [README](job/README.md). |

TPC-H (`--dataset TPCH`) is still defined in Scala; it will move here too.

## Adding a dataset

Create a folder `datasets/<name>/` with:

```
datasets/<name>/
  schema.sql        CREATE TABLE statements
  queries/*.sql     seed queries
  dataset.json      optional: file format and overrides
```

Then derive the dataset's catalog and input workload, measuring the statistics on the data:

```bash
python -m datafarm.datasets.add_dataset datasets/<name> --data /path/to/data
python RunGenerator.py --dataset <name> --platform Spark --plans 10 --versions 2
```

`add_dataset` writes two things next to your files:

- **`catalog.json`:** what the job generator reads: the tables and columns, the join graph, the filter candidates,
  the group-by fields and the statistics. You can edit it, but your changes are lost the next time you run
  `add_dataset`. Put lasting changes in `dataset.json` instead.
- **`workload/`:** one input plan per seed query. DataFarm learns the shape of the jobs from these plans: their
  operators, the number of joins and the depth.

The data files must be at `<data path>/<table>.csv` (or as set in `dataset.json`). The generated jobs read them from
the data path you give at run time, and `load_sql_data.py --dataset <name>` loads them into DuckDB, PostgreSQL or SQL
Server.

### schema.sql

`CREATE TABLE` statements with column types, `NOT NULL`, `PRIMARY KEY`, and foreign keys (`REFERENCES` on a column, or
`FOREIGN KEY ... REFERENCES ...`). Other statements and constraints are ignored. A table can have at most 22 columns,
because the generated jobs hold each row in a Scala tuple.

Column types are read as one of two kinds:

- **Integer types** (`INTEGER`, `BIGINT`, `SMALLINT`, ...) become 64-bit integers. A value that is not an integer is
  NULL.
- **Everything else** is text. `CHAR`, `VARCHAR` and `TEXT` columns are text anyway. In this version, other types
  (`DATE`, `TIMESTAMP`, `DECIMAL`, ...) are carried as their text too. Dates and timestamps in ISO format still order
  correctly, so range filters on them are kept. Columns such as `DECIMAL` only get equality, `IN`, `LIKE` and NULL
  filters. Proper decimal and date types are planned.

### Seed queries

Seed queries are SQL queries over your tables, one per `.sql` file, written the way your users or your benchmark would
query the data:

```sql
SELECT MIN(p.name)
FROM products p, order_items i, orders o
WHERE p.id = i.product_id
  AND i.order_id = o.id
  AND p.category IN ('books', 'games')
  AND o.order_date >= DATE '2024-01-01';
```

Supported: `SELECT [DISTINCT] ... FROM` with comma joins or `[INNER] JOIN ... ON`, and table aliases. Also `WHERE`,
`GROUP BY`, `HAVING`, `ORDER BY` and `LIMIT`. Subqueries, `WITH`, `UNION` and outer joins are not supported, and such
queries are skipped with a message. From the seed queries `add_dataset` takes:

- **Joins:** every equi-join `a.x = b.y` between two tables is an edge of the join graph. The more queries use an
  edge, the earlier it is listed. Foreign keys in `schema.sql` add edges that no query uses.
- **Filters:** every filter on one table becomes a filter candidate of its column, with the number of queries that
  use it. This includes comparisons with a constant, `BETWEEN`, `IN`, `LIKE`, `NOT LIKE` and `IS [NOT] NULL`. A
  disjunction like `x LIKE 'a%' OR x LIKE 'b%'` gives one candidate per disjunct. Other predicates (functions, `OR`
  over different columns, ...) are reported and skipped. Tables that no query filters get `id % m = r` filters on an
  integer key.
- **Input plans:** each query becomes a plan in the style of a hand-written Flink job. Every table is a `Data Source`
  with a `Filter` if the query filters it, and the joins form a left-deep chain. An aggregate without `GROUP BY`
  becomes a `Reduce` pair (one row), and `GROUP BY` or `DISTINCT` becomes a `Group by` pair (one row per key).
  `ORDER BY` becomes a `Sort-Partition`.
- **Table repetitions:** how often a table occurs at most in one query. This limits how often a generated job may
  join the same table.

More, and more varied, seed queries give more varied jobs.

### dataset.json

Optional settings, for example:

```json
{
  "about": "What the dataset is.",
  "format": {"extension": ".csv", "delimiter": ",", "quote": "\"", "escape": null, "null": "", "header": true},
  "files": {"orders": "orders_2024.csv"},
  "groupFields": {"orders": ["customer_id", "status"]},
  "joins": [["orders.customer_id", "customers.id"]],
  "maxOccurrences": {"customers": 2},
  "exclude": ["audit_log"]
}
```

| Setting | Meaning | Default |
|---|---|---|
| `format.extension` | file name extension of the data files | `.csv` |
| `format.delimiter` | field separator, one character | `,` |
| `format.quote` | quote character, or `null` for none | `"` |
| `format.escape` | escape character inside quotes, or `null` (a doubled quote is always one quote) | `null` |
| `format.null` | an unquoted field with this text is NULL | `""` (empty field) |
| `format.header` | the files start with a header line | `false` |
| `files` | data file of a table, if not `<table><extension>` | |
| `groupFields` | the group-by fields of a table | derived, see below |
| `joins` | join edges that no seed query uses | |
| `maxOccurrences` | how often a table may occur in one job | from the seed queries, at least 1 |
| `exclude` | tables of `schema.sql` that jobs never read | |

The files are read by the same rules on every platform (`datafarm/datasets/csvformat.py`, and its counterpart in the
parsers generated into the jobs). A row is one line. A field that starts with the quote character runs to the closing
quote. An unquoted field equal to `format.null` is NULL, but a quoted one never is. Missing trailing fields are NULL,
and extra ones are ignored, so `|`-terminated lines such as TPC-H's work with `"delimiter": "|"`.

**Group-by fields** are the columns generated group-by operators group on. Without `groupFields`, they are:

1. the table's join columns that are not its primary key (the keys of its relationships);
2. with `--data`, columns with at least 2 and at most rows / 10 distinct values;
3. if neither gives any, the first column that is not the primary key.

At most 4 are used.

## What the generated jobs do

The jobs of these datasets compute the same result on every platform. The CI checks this on `example/` by comparing
the row counts of every job on Flink, Wayang (Java and Spark), Spark, DuckDB and PostgreSQL.

| Operator | Semantics |
|---|---|
| Data Source | reads and parses the table's file, skipping the header line if there is one |
| Filter | one filter candidate of the table. As in SQL, NULL satisfies no predicate except `IS NULL` |
| Join | an edge of the join graph, keeping the right-hand row. NULL keys match nothing, as in SQL |
| Group by | one row per value of a group-by field: the smallest row, comparing all columns in order with NULLs first. NULL text keys group with `""` |
| Reduce | the smallest row of the whole input |
| Sort-Partition | sorts on a column without NULLs |
| Map, Partition | identity, with the UDF cost of the map (none in SQL) |

`generated_jobs_info.json` records each operator with the measured selectivity of its filter and the number of
distinct values of its group-by field. Values that were not measured (no `--data`) are recorded as -1.
