from CONFIG import CONFIG
import glob
import json
import os
import pickle


def generate_abstract_plans():
    print(
        f"|--> Generating '{CONFIG.N_JOBS}' abstract plans in '{CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER}' "
        f"from the input workload '{CONFIG.ORIG_EXEC_PLAN_FOLDER}'"
    )
    print()

    import sys
    python_executable = sys.executable
    os.system(
        f"cd {CONFIG.PROJECT_PATH}; "
        f"{python_executable} -m datafarm.abstract_generation.generate_abstract_plans {CONFIG.N_JOBS} {CONFIG.ORIG_EXEC_PLAN_FOLDER} {CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER} "
        f"{CONFIG.ORIG_EXEC_PLAN_FILTER} {CONFIG.ABSTRACT_PLAN_MAX_DEPTH} {CONFIG.ABSTRACT_PLAN_MAX_JOINS}"
    )


def generate_jobs():
    print(f"|--> Generating '{CONFIG.N_JOBS}' jobs in '{CONFIG.GENERATED_JOB_FOLDER}'")
    print()

    os.system(
        f"cd {CONFIG.JOB_GENERATOR}; "
        f"DATAFARM_SQLSERVER_TABLE_PREFIX='{CONFIG.SQLSERVER_TABLE_PREFIX}' "
        f'sbt "runMain instantiation.JobGenerator {CONFIG.N_JOBS} {CONFIG.N_VERSIONS} {CONFIG.DATA_MANAGER} {CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER} {CONFIG.GENERATED_JOB_FOLDER} {CONFIG.JOB_SEED} {CONFIG.TARGET_PLATFORM}"'
    )


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


if __name__ == "__main__":
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
