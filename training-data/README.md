# training-data

`RunGenerator.py` writes its output here, one folder per experiment. The folder is named after `--experiment`, or
`<platform>_<dataset>` by default (e.g. `SQL_JOB/`). Everything in this folder except this file is ignored by git.

```
<experiment>/
  generated_abstract_exec_plans/       abstract plans learned from the input workload (plan_N.json)
  generated_jobs/                      the generated jobs, JobNvM for abstract plan N, version M
    generated_jobs_info.json           every operator of every job: tables, filters and their selectivities,
                                       joins, group-by fields
  generated_jobs_exec_plans/           labeling only: execution plans of the jobs that were run
  generated_jobs_task_manager_details/ labeling only: Flink task-manager details of those runs
  label_forecaster_<timestamp>/        labeling only: final_dataset.csv with measured or predicted run times
```

What `generated_jobs/` contains depends on the platform:

| Platform | Jobs |
|---|---|
| `Flink` | one sbt project per job (`JobNvM/`) |
| `Wayang` | `JobNvM.scala` |
| `Spark` | `JobNvM.scala`, plus `build.sbt` to build them all with `sbt package` |
| `SQL`, `SQLServer` | `JobNvM.sql`, plus `schema.sql` with the tables |
| `SQLServerFlat` | `JobNvM.txt` (query), `JobNvM.json` and `JobNvM.pickle` (its description) |

See the main [README](../README.md) for the options and for how to run the jobs of each platform.
