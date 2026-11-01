"""
Generate jobs: learn abstract plans from an input workload, then instantiate them for a platform and dataset.

    python RunGenerator.py --platform SQL --dataset JOB --plans 50 --versions 4

writes 50 x 4 jobs to training-data/SQL_JOB/generated_jobs/. Options left out take their value from CONFIG.py.
Needs Python with requirements.txt and sbt (the job generator is a Scala project).
"""
import argparse
import glob
import json
import os
import pickle
import shutil
import subprocess
import sys

from CONFIG import CONFIG


def run(cmd, cwd, env=None, what=""):
    result = subprocess.run(cmd, cwd=cwd, env=env)
    if result.returncode != 0:
        sys.exit(f"Error: {what} failed (exit code {result.returncode}); see the output above.")


def generate_abstract_plans():
    print(
        f"|--> Generating '{CONFIG.N_JOBS}' abstract plans in '{CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER}' "
        f"from the input workload '{CONFIG.ORIG_EXEC_PLAN_FOLDER}'"
    )
    print()
    run(
        [sys.executable, "-m", "datafarm.abstract_generation.generate_abstract_plans", str(CONFIG.N_JOBS),
         CONFIG.ORIG_EXEC_PLAN_FOLDER, CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER, CONFIG.ORIG_EXEC_PLAN_FILTER,
         str(CONFIG.ABSTRACT_PLAN_MAX_DEPTH), str(CONFIG.ABSTRACT_PLAN_MAX_JOINS)],
        cwd=CONFIG.PROJECT_PATH, what="Abstract plan generation",
    )


def generate_jobs():
    print(f"|--> Generating '{CONFIG.N_JOBS}' x '{CONFIG.N_VERSIONS}' jobs in '{CONFIG.GENERATED_JOB_FOLDER}'")
    print()
    if shutil.which("sbt") is None:
        sys.exit("Error: sbt not found. The job generator is a Scala project and needs sbt (https://www.scala-sbt.org/).")
    env = dict(os.environ, DATAFARM_SQLSERVER_TABLE_PREFIX=CONFIG.SQLSERVER_TABLE_PREFIX)
    main = " ".join(["runMain instantiation.JobGenerator", str(CONFIG.N_JOBS), str(CONFIG.N_VERSIONS), CONFIG.DATA_MANAGER,
                     CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER, CONFIG.GENERATED_JOB_FOLDER, str(CONFIG.JOB_SEED),
                     CONFIG.TARGET_PLATFORM])
    run(["sbt", main], cwd=CONFIG.JOB_GENERATOR, env=env, what="Job generation")


def write_ltr_pickles(job_folder):
    """SQLServerLTR: the dict of each JobXvY.json as JobXvY.pickle, the file the LTR featurizers load."""
    files = [f for f in glob.glob(os.path.join(job_folder, "Job*v*.json"))]
    for f in files:
        with open(f) as fh:
            query = json.load(fh)
        with open(f[: -len(".json")] + ".pickle", "wb") as fh:
            pickle.dump(query, fh)
    print(f"|--> Wrote {len(files)} .pickle query descriptions for the LTR pipeline")


def create_project_folders():
    project_folders = [
        CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER,
        CONFIG.GENERATED_JOB_FOLDER,
        CONFIG.GENERATED_JOB_EXEC_PLAN_FOLDER,
        os.path.join(CONFIG.GENERATED_JOB_EXEC_PLAN_FOLDER, CONFIG.DATA_ID),
        os.path.join(CONFIG.GENERATED_JOB_TASK_MANAGER_DETAILS, CONFIG.DATA_ID),
    ]

    for pj_f in project_folders:
        pj_f = os.path.abspath(pj_f)
        if not os.path.exists(pj_f):
            print(f"|--> Creating project folder: {pj_f}")
            os.makedirs(pj_f)
        else:
            print(f"|--> Skip project folder: {pj_f}")


def parse_args(argv=None):
    platforms = sorted({p for ps in CONFIG.SUPPORTED_PLATFORMS.values() for p in ps})
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--platform", choices=platforms, default=CONFIG.TARGET_PLATFORM,
                    help=f"target platform of the jobs (default: {CONFIG.TARGET_PLATFORM})")
    ap.add_argument("--dataset", choices=sorted(CONFIG.SUPPORTED_PLATFORMS), default=CONFIG.DATA_MANAGER,
                    help=f"dataset the jobs run on (default: {CONFIG.DATA_MANAGER})")
    ap.add_argument("--plans", type=int, default=CONFIG.N_JOBS,
                    help=f"number of abstract plans (default: {CONFIG.N_JOBS})")
    ap.add_argument("--versions", type=int, default=CONFIG.N_VERSIONS,
                    help=f"jobs per abstract plan; total jobs = plans x versions (default: {CONFIG.N_VERSIONS})")
    ap.add_argument("--seed", type=int, default=CONFIG.JOB_SEED,
                    help=f"job seed, the same seed gives the same jobs; -1 = from the plan index (default: {CONFIG.JOB_SEED})")
    ap.add_argument("--experiment", default=CONFIG.EXPERIMENT_ID,
                    help="name of the output folder (default: <platform>_<dataset>)")
    ap.add_argument("--output", default=CONFIG.OUTPUT_PATH,
                    help="folder the experiment folder is created in (default: training-data/ in the repository)")
    ap.add_argument("--table-prefix", default=CONFIG.SQLSERVER_TABLE_PREFIX,
                    help='SQLServerLTR only: prefix of table names, e.g. "tpch.dbo." (default: none)')
    args = ap.parse_args(argv)
    supported = CONFIG.SUPPORTED_PLATFORMS[args.dataset]
    if args.platform not in supported:
        ap.error(f"dataset {args.dataset} supports the platforms {', '.join(supported)}, not {args.platform}")
    if args.plans < 1 or args.versions < 1:
        ap.error("--plans and --versions must be at least 1")
    return args


if __name__ == "__main__":
    args = parse_args()
    CONFIG.update(
        TARGET_PLATFORM=args.platform, DATA_MANAGER=args.dataset, N_JOBS=args.plans, N_VERSIONS=args.versions,
        JOB_SEED=args.seed, EXPERIMENT_ID=args.experiment,
        OUTPUT_PATH=os.path.abspath(args.output) if args.output else "",
        SQLSERVER_TABLE_PREFIX=args.table_prefix,
    )

    print("|Init project")
    create_project_folders()

    print()
    print("|Abstract Plan Generation")
    generate_abstract_plans()

    print()
    print("|Job Generation")
    generate_jobs()

    if CONFIG.TARGET_PLATFORM == "SQLServerLTR":
        write_ltr_pickles(CONFIG.GENERATED_JOB_FOLDER)

    print()
    print(f"|Done: {CONFIG.N_JOBS * CONFIG.N_VERSIONS} {CONFIG.TARGET_PLATFORM} jobs on {CONFIG.DATA_MANAGER} in "
          f"{CONFIG.GENERATED_JOB_FOLDER} (generated_jobs_info.json there describes each job)")
