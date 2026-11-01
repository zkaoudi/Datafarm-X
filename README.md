# DataFarm-X

DataFarm-X generates and labels large workloads of realistic jobs for ML-based data management (learned cost
models, learned query optimizers, ...). It extends [DataFarm](#citation), which generated Flink jobs, to several
platforms ("X"): **Flink, Apache Wayang, Spark, SQL (PostgreSQL, DuckDB) and SQL Server**, over the **TPC-H** and
**Join Order Benchmark (JOB)** datasets.

DataFarm follows a data-driven, white-box approach. It learns the patterns of a small input workload (operator
sequences, number of joins, plan depth) as *abstract plans*, then instantiates each abstract plan into concrete jobs
by choosing tables, joins, filters and group-by fields from the dataset's metadata. A label forecaster then runs a
few of the jobs and predicts the run times of the others with active learning.

For a given plan and seed, every platform gets the same tables, joins, filters and group-by fields. The workloads
are therefore comparable across platforms, for example the same job as a Wayang plan, a Spark program and a SQL query.

## Contents
- [Platforms and datasets](#platforms-and-datasets)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- Platforms: [Flink](#flink) · [Wayang](#wayang) · [Spark](#spark) · [SQL](#sql) · [SQL Server](#sql-server)
- Datasets: [TPC-H](#tpc-h) · [JOB](#join-order-benchmark-job) · [IMDB (Kaggle)](#imdb-kaggle)
- [Labeling jobs](#labeling-jobs)
- [Citation](#citation)

## Platforms and datasets

Choose the platform with `TARGET_PLATFORM` and the dataset with `DATA_MANAGER` in `CONFIG.py`.

| `TARGET_PLATFORM` | Output | TPC-H | JOB |
|---|---|:---:|:---:|
| `Flink` | an sbt project per job (Flink DataSet API) | ✓ | |
| `Wayang` | `JobXvY.scala` (Wayang Scala API) | ✓ | ✓ |
| `Spark` | `JobXvY.scala` (RDD API) and a `build.sbt` | ✓ | ✓ |
| `SQL` | `JobXvY.sql` (standard SQL) and `schema.sql` | ✓ | ✓ |
| `SQLServer` | `JobXvY.sql` (T-SQL) and `schema.sql` | ✓ | ✓ |
| `SQLServerLTR` | `JobXvY.txt` and `JobXvY.pickle` (flat queries for an LTR optimizer) | ✓ | |

The `IMDB` data manager (the Kaggle IMDb dump) comes from the original DataFarm and supports Flink only.

## Requirements

- **Generator:** sbt ≥ 1.3 with Scala 2.11, and Python 3.6–3.8 with the packages in `requirements.txt`
  (`pip install -r requirements.txt`).
- **Running the generated jobs:** the platform itself. See each platform's section for versions.

DataFarm-X has been tested on Linux and macOS. `docker-compose.yml` sets up a container with sbt, Scala and Python;
the default paths in `CONFIG.py` match that container.

## Quick start

1. Edit `CONFIG.py` (see [Configuration](#configuration)): set the paths, `DATA_MANAGER` and `TARGET_PLATFORM`.
2. Run `RunGenerator.py`. It learns abstract plans from the input workload of the chosen dataset and generates
   `N_JOBS × N_VERSIONS` jobs in `data/<EXPERIMENT_ID>/generated_jobs/`, plus `generated_jobs_info.json` with the
   metadata of every operator (tables, selectivities, cardinalities).
3. Run the jobs on the platform (see its section), or label them with `RunLabelForecaster.py`
   (see [Labeling jobs](#labeling-jobs)).

## Configuration

Paths in `CONFIG.py`:

- `PROJECT_PATH`: absolute path to this repository.
- `GENERATED_JOB_INPUT_DATA_PATH`: absolute path to the folder with your input data (used when running jobs).
- `FLINK_HOME`: the Flink installation (the folder containing `bin/flink`); only needed for Flink and labeling.

Generator settings:

- `N_JOBS`: number of abstract plans to generate.
- `N_VERSIONS`: number of jobs instantiated from each abstract plan.
- `JOB_SEED`: seed for reproducible generation. With `-1`, each job's seed is derived from its plan index (also
  deterministic); otherwise it is derived from `JOB_SEED` and the plan index, so different plans still get
  different tables, filters and group-by fields.
- `DATA_MANAGER`: `"TPCH"`, `"JOB"` or `"IMDB"` (see the dataset sections).
- `TARGET_PLATFORM`: `"Flink"`, `"Wayang"`, `"Spark"`, `"SQL"`, `"SQLServer"` or `"SQLServerLTR"` (see the platform sections).
- `INPUT_WORKLOAD`: per data manager, the folder of input execution plans, a file-name filter, and the maximum
  plan depth and number of joins (`-1` = learned from the workload).
- `DATA_ID`: which entry of `TableMetaData.py` (TPC-H scale factor) to use for labeling features.
- `EXPERIMENT_ID`: name of the folder in `data/` where the results are stored.
- `SQLSERVER_TABLE_PREFIX`: table-name prefix for `SQLServerLTR`, e.g. `"tcph.dbo."` (empty = unqualified).

Label forecaster settings are described in [Labeling jobs](#labeling-jobs).

---

## Platforms

### Flink

The original DataFarm target. Each job becomes an sbt project `JobXvY/` (`build.sbt`, `project/plugins.sbt` with
sbt-assembly, `src/main/scala/JobXvY.scala`) using the Flink DataSet API.

- **Versions:** Flink 1.10 with Scala 2.11. Flink 1.10 runs on Java 8 or 11, not on Java 17 or later.
- **Running a job:**
  `JobXvY <data_path> <exec_plan_out_path> <exec|noexec> <local|nolocal> <heap_size> <parallelism>`.
  With `exec`, the job runs and writes `<exec_plan_out_path>/JobXvY$.json` with Flink's execution plan and the net
  run time. `BuildAndSubmit.py` builds the projects with `sbt assembly` and submits them with `flink run`.
- **Datasets:** TPC-H (and IMDB from the original DataFarm).

### Wayang

Each job is one file, `JobXvY.scala`: a class in package `org.apache.wayang.ml.generatables` that extends
`GeneratableJob`. Its `buildPlan(args)` builds the plan with the Wayang Scala API and returns its final `DataQuanta`
(`args(0)`: platform plugins, `args(1)`: data path).

- **Wayang version:** the jobs use classes of a Wayang fork with an ML training module (`GeneratableJob`,
  `withUdfComplexity`, `collectNoExecute`, and `toTuple` on the TPC-H data classes), which the public
  `apache/wayang` release does not have.
- **Datasets:** TPC-H uses the TPC-H data classes of `wayang-benchmark` (`org.apache.wayang.apps.tpch.data`). JOB
  jobs bring their own parser object (`JobXvYParsers` in the same file), so no JOB classes are needed in Wayang.

### Spark

Each job is a standalone Scala object in package `datafarm.generated.spark`, using the RDD API with one
transformation per plan operator. Spark therefore runs exactly the generated plan: Catalyst does not re-optimize
filters or join order.

- **Versions:** the generator writes a `build.sbt` for Spark 3.5.3 and Scala 2.12 (`provided`) next to the jobs.
- **Running a job:**

  ```bash
  cd data/<EXPERIMENT_ID>/generated_jobs
  sbt package
  spark-submit --class datafarm.generated.spark.Job0v0 target/scala-2.12/datafarm-spark-jobs_2.12-0.1.jar \
    /path/to/data/ [planOutputDir]
  ```

  The data path must end with `/`. A job runs its plan with `count()` and prints the row count and run time. With
  `planOutputDir`, it also writes `JobXvY.json` with the run time, row count and RDD lineage (`toDebugString`).
- **Parsing:** each job parses its own input (object `JobXvYParsers` in the same file): TPC-H `.tbl` files or the
  JOB CSVs. TPC-H dates become `Int`s in the encoding of Wayang's `CsvUtils.parseDate`, so date filters behave the
  same as on Wayang.
- **Semantics:** joins keep the right-hand tuple, as in the Wayang jobs. A Reduce pair in the abstract plan is a
  keyed reduce on a group field for TPC-H (as in the Flink TPC-H jobs) and a global reduce for JOB (JOB's `MIN(...)`
  aggregate).

### SQL

With `TARGET_PLATFORM = "SQL"`, each job is one standard-SQL statement in `JobXvY.sql`, tested with PostgreSQL 16
and DuckDB.

- **Structure:** every plan operator becomes a common table expression named like the variables of the dataflow
  jobs (`source0`, `filter1`, `join2`, ...), and the job returns the output of the last one.
- **Schema:** the generator writes `schema.sql` next to the jobs: the `CREATE TABLE` statements, with load commands
  for PostgreSQL and DuckDB in its header. TPC-H uses dbgen's column names (`l_orderkey`, ...), JOB those of its
  `schema.sql`.
- **Operators:** joins keep the right-hand rows, as in the dataflow jobs. A group-by (or keyed Reduce) returns one
  row per key with `MIN` of each other column, and a global Reduce one row with `MIN` of each column. Maps and
  partitions pass their input through, since their UDF cost has no SQL counterpart.
- **Optimization:** unlike Wayang, Flink and Spark, which run the generated plan as written, the database optimizes
  the query: it may reorder joins and push down filters. That suits training data for learned query optimizers.
- **Result sizes:** the dataflow jobs keep an arbitrary row per group, whereas SQL takes the per-column minimum. Jobs
  that join, filter or group the output of a group-by again can therefore return different result sizes than on
  the dataflow platforms (10 of 80 TPC-H jobs in our tests, none of the JOB jobs).

### SQL Server

Two targets generate jobs for Microsoft SQL Server.

**`TARGET_PLATFORM = "SQLServer"`** (TPC-H and JOB) writes the [SQL](#sql) jobs in T-SQL:

- dates as `CAST('…' AS DATE)`;
- sorts inside a common table expression with `OFFSET 0 ROWS` (SQL Server rejects a plain `ORDER BY` there);
- `[` escaped in `LIKE` patterns.

Its `schema.sql` uses, for TPC-H, the column types of dbgen's `dss.ddl` and the primary keys of `dss.ri`. For JOB,
it uses the lengths of JOB's `schema.sql` as `NVARCHAR`, with a binary collation, so text comparisons are
case-sensitive as on the other platforms (SQL Server's default collations are not).

**`TARGET_PLATFORM = "SQLServerLTR"`** (TPC-H only) writes flat queries for the learning-to-rank optimizer of
[LTR_cross_hardwares](https://github.com/XLI-2020/LTR_cross_hardwares), in the format of its DataFarm port:

- `JobXvY.txt` holds the query (`SELECT … FROM t1, t2 WHERE … GROUP BY … ORDER BY …`).
- `JobXvY.pickle`, written by `RunGenerator.py` from `JobXvY.json`, holds the same query as a dict with the keys
  `FROM`, `SELECT`, `WHERE`, `WHERE_JOIN`, `GROUP BY` and `ORDER BY`.
- Filters, joins and group-bys come from the plan. Maps become columns or `COUNT`/`AVG`/`SUM`, and sorts become
  `ORDER BY` items, using that port's rules.
- Unlike that port, every query is valid SQL Server: no table occurs twice in `FROM` (the format has no aliases),
  and grouped queries select and sort only group columns and aggregates.
- Set `SQLSERVER_TABLE_PREFIX` in `CONFIG.py` (e.g. `"tcph.dbo."`) to qualify the table names.
- JOB is not supported: the format stores one column name per join, so `cast_info.movie_id = title.id` cannot be
  expressed.

**Loading data and running jobs** (needs `pip install pyodbc` and an ODBC driver for SQL Server):

```bash
# create the tables and load the data (BULK INSERT cannot read JOB's backslash-escaped CSVs)
python sqlserver/load_sqlserver.py --dataset JOB --data /path/to/imdb --schema data/<EXPERIMENT_ID>/generated_jobs/schema.sql \
    --connection "DRIVER={ODBC Driver 18 for SQL Server};SERVER=…;DATABASE=…;UID=…;PWD=…;TrustServerCertificate=yes"
# run all jobs of a folder (JobXvY.sql or JobXvY.txt), or save their estimated plans with --showplan <dir>
python sqlserver/run_sql_jobs.py data/<EXPERIMENT_ID>/generated_jobs --output results.csv --connection "…"
```

The connection string can also be set in the environment variable `DATAFARM_SQLSERVER`.

---

## Datasets

### TPC-H

`DATA_MANAGER = "TPCH"`. All platforms.

- **Data:** the `.tbl` files generated by TPC-H's dbgen (`|`-separated). A sample of about 1 GB is available
  [on Kaggle](https://www.kaggle.com/fven7u/tpch-1gb).
- **Input workload:** `data/input_workload_exec_plan/`, Flink execution plans of TPC-H queries 1, 3, 11, 13, 17 and
  21 (hand-written and SQL versions). Generated plans have at most depth 6 and 3 joins (`INPUT_WORKLOAD` in
  `CONFIG.py`). You can add any execution plan extracted from Flink jobs.
- **Metadata:** each table's join fields, filter fields with values and selectivities, and group-by fields with
  their number of distinct values are defined in the table managers
  (`instantiator/src/main/scala/Generator/DatasetJob/tpc_h/table/`). `TableMetaData.py` holds the table
  cardinalities for scale factors 1, 5, 10 and 50 GB, which the label forecaster uses.

### Join Order Benchmark (JOB)

`DATA_MANAGER = "JOB"`. The IMDB database of the Join Order Benchmark
([Leis et al., PVLDB 2015](https://www.vldb.org/pvldb/vol9/p204-leis.pdf)), for Wayang, Spark, SQL and SQLServer.

- **Data:** the 21 IMDB CSV files of the original JOB dump (`title.csv`, `cast_info.csv`, ...). In the dataflow
  jobs, integer columns become `Long` (NULL is `Long.MinValue`) and text columns `String` (NULL is `null`).
- **Schema and joins:** tables, columns and join edges follow the JOB schema
  ([gregrahn/join-order-benchmark](https://github.com/gregrahn/join-order-benchmark)). Joins use the equi-join edges
  that occur in the 113 JOB queries, including the direct `movie_id`/`person_id` joins between fact tables.
- **Filters:** the filters are taken from the JOB queries (e.g. `company_name.country_code = '[us]'`,
  `title.production_year > 2000`, `keyword.keyword in (...)`). Tables that JOB never filters directly
  (`aka_title`, `complete_cast`, `movie_keyword`, `movie_link`) get `id % m = 0` predicates. All of this lives in
  `instantiator/src/main/resources/job/job_catalog.json`; edit it to add or remove predicates.
- **Input workload:** the abstract plans are learned from the JOB queries, not TPC-H.
  `data/input_workload_exec_plan_job/` holds one plan per JOB query (113), built by
  `generator_labeler/JOB_InputWorkload.py` from the query text in the style of a hand-written Flink DataSet job: per
  table a source and, if filtered, a filter; a left-deep chain of joins; the `MIN(...)` aggregate as a reduce; a sink.
  These plans describe only the operator graph; they were not produced by running Flink. Plan depth and number of
  joins are learned from this workload, so generated jobs have 1 to about 16 joins, as in JOB. Tables are reused at
  most as often as in a single JOB query. To rebuild the plans:

  ```bash
  git clone https://github.com/gregrahn/join-order-benchmark
  python generator_labeler/JOB_InputWorkload.py join-order-benchmark data/input_workload_exec_plan_job
  ```

- **Statistics:** selectivities, distinct counts and table sizes depend on your IMDB snapshot. Only the values of the
  small lookup tables are filled in; the rest is `null` (recorded as `-1` in `generated_jobs_info.json`) until you
  measure it:

  ```bash
  python generator_labeler/DatasetMetadata/JOB_MetaDataAnalyzer.py /path/to/imdb_csv_dir
  ```

  This writes the measured values into the catalog. Use `--output` to write elsewhere, and point the generator to
  that file with the environment variable `DATAFARM_JOB_CATALOG`. The analyzer uses the same parsing and NULL
  semantics as the generated jobs.
- **Limitations:**
  - The generator keeps only the right-hand side of each join, so a job is a chain of joins in which each join uses
    a column of the table joined last. Star-shaped JOB queries therefore become chains, mostly through the
    `movie_id` joins between fact tables.
  - JOB queries contain only sources, filters, joins and an aggregate, so JOB-based jobs contain no map, group-by or
    sort operators.
  - Joins over fact tables on `movie_id` are many-to-many (as in JOB) and can produce very large intermediate
    results when no selective filter precedes them.

### IMDB (Kaggle)

`DATA_MANAGER = "IMDB"`. The IMDb dump from Kaggle (`title.basics`, `title.ratings`, `name.basics`, ...), inherited
from the original DataFarm. Flink only, and not maintained or tested in DataFarm-X. For IMDB data, prefer the JOB
data manager.

---

## Labeling jobs

`RunLabelForecaster.py` labels the generated jobs with their run times. It runs an initial sample of jobs, trains a
quantile regression forest on their run times, and then repeatedly runs the jobs it is most uncertain about until
the model's uncertainty stops improving. The result is `final_dataset.csv` with measured or predicted run times for
every job.

It currently submits **Flink** jobs only (through `BuildAndSubmit.py` and a running Flink cluster). For the other
platforms, run the jobs yourself: Spark jobs and `sqlserver/run_sql_jobs.py` report run times.

Settings in `CONFIG.py`:

- `INIT_JOBS`: number of jobs sampled and run before active learning starts.
- `MAX_ITER`: maximum number of active-learning iterations.
- `MAX_EARLY_STOP`: number of early-stop signals before the process stops.
- `EARLY_STOP_TH`: early-stop threshold, between 0 and 1.
- `RANDOM_INIT`: sample the initial jobs at random instead of by clustering.
- `RANDOM_SAMPLING`: pick the jobs to run by weighted random sampling based on uncertainty.

## Citation

DataFarm-X extends DataFarm:

```
F. Ventura, Z. Kaoudi, J. Quiané-Ruiz, and V. Markl. Expand your Training Limits! Generating and Labeling Jobs
for ML-based Data Management. In SIGMOD, 2021.
```
