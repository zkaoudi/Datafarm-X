# Join Order Benchmark (JOB)

The IMDB database of the Join Order Benchmark:

> V. Leis, A. Gubichev, A. Mirchev, P. Boncz, A. Kemper, T. Neumann. *How Good Are Query Optimizers, Really?*
> PVLDB 9(3), 2015.

```bash
python RunGenerator.py --dataset job --platform SQL --plans 10 --versions 2
```

This folder holds the schema (`schema.sql`), the settings (`dataset.json`), and the `catalog.json` that `add_dataset`
derived from the 113 JOB queries. The JOB input workload, the queries translated into plans, is in
[`input-workloads/job/`](../../input-workloads/job/). The queries and the data are not included.

- **Data:** the 21 CSV files of the IMDB snapshot used in the paper (`title.csv`, `cast_info.csv`, ...), from
  http://event.cwi.nl/da/job/imdb.tgz. They are comma separated, with text quoted with `"`, `\` as the escape
  character, and an empty unquoted field as NULL (see `format` in `dataset.json`).
- **Queries:** `1a.sql` ... `33c.sql` of https://github.com/gregrahn/join-order-benchmark.
- **Group-by fields:** chosen by hand in `dataset.json`.

The shipped `catalog.json` has no statistics, so selectivities and distinct counts are recorded as -1. To measure them
on your copy of the data (one pass per table; `cast_info` has about 36 million rows):

```bash
git clone https://github.com/gregrahn/join-order-benchmark /tmp/jb
mkdir /tmp/jb-queries && cp /tmp/jb/[0-9]*.sql /tmp/jb-queries/
python -m datafarm.datasets.add_dataset datasets/job --queries /tmp/jb-queries --data /path/to/imdb
```

With the queries at hand, `--workload /tmp/jb-queries` uses them as the input workload directly. That gives the same
plans as `input-workloads/job/`, which CI checks.

Notes on the generated jobs:

- They have 1 to about 16 joins, as the JOB queries do. A table is used at most as often as it occurs in a single JOB
  query, for example `info_type` twice.
- Joins over fact tables on `movie_id` are many-to-many, as in JOB, and can produce very large intermediate results.
  Expect some generated jobs to run for a long time.
- The JOB queries only scan, filter, join and aggregate, so the jobs contain no map, group-by or sort operators.
