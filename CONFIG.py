from datetime import datetime
import os
from os.path import abspath, dirname, join

# Settings for generating (RunGenerator.py) and labeling (RunLabelForecaster.py) jobs. The settings of the
# job generator can also be given on the command line: python RunGenerator.py --help


class CONFIG:
    # PATH TO DATA-FARM PROJECT (the folder of this file)
    PROJECT_PATH = dirname(abspath(__file__))

    # PATH TO INPUT DATA
    GENERATED_JOB_INPUT_DATA_PATH = "/data"  # <absolute_path_to_input_data>

    # FLINK HOME
    FLINK_HOME = "/opt/flink"  # <path_to_flink> (the folder that contains bin/flink)

    # EXPERIMENT ID: name of the output folder training-data/<EXPERIMENT_ID>/ ("" = <TARGET_PLATFORM>_<DATA_MANAGER>)
    EXPERIMENT_ID = ""
    # DATA ID
    DATA_ID = "1GB"

    # Job generator conf
    N_JOBS = 10  # number of abstract plans
    N_VERSIONS = 2  # jobs per abstract plan (different tables, filters, group-by fields): N_JOBS * N_VERSIONS jobs
    JOB_SEED = 42  # fixed seed = reproducible jobs; -1 = seed from the plan index only
    DATA_MANAGER = "TPCH"  # "TPCH" | "IMDB" | a dataset folder in datasets/, e.g. "JOB" or "example" (see datasets/README.md)
    TARGET_PLATFORM = "SQL"  # "Flink" | "Wayang" | "Spark" | "SQL" | "SQLServer" | "SQLServerFlat"
    # SQLServerFlat: prefix of the table names in the generated queries, e.g. "tpch.dbo." ("" = unqualified)
    SQLSERVER_TABLE_PREFIX = ""
    # where the generated output goes ("" = <PROJECT_PATH>/training-data/)
    OUTPUT_PATH = ""
    #################################################################

    # Datasets defined by a folder datasets/<name>/ with a catalog.json (python -m datafarm.datasets.add_dataset)
    DATASETS_PATH = join(PROJECT_PATH, "datasets")
    # Platforms every dataset defined by a folder supports
    DATASET_PLATFORMS = ["Flink", "Wayang", "Spark", "SQL", "SQLServer"]

    # Platforms each built-in DATA_MANAGER supports
    SUPPORTED_PLATFORMS = {
        "TPCH": ["Flink", "Wayang", "Spark", "SQL", "SQLServer", "SQLServerFlat"],
        "IMDB": ["Flink"],
    }

    # Input workload the abstract plans are learned from, per DATA_MANAGER:
    # (folder, plan-file name filter, max plan depth, max joins); -1 = learned from the workload
    INPUT_WORKLOAD = {
        "TPCH": ("input-workloads/tpch/", "TPC_H", 6, 3),
        "IMDB": ("input-workloads/tpch/", "TPC_H", 6, 3),
    }

    @classmethod
    def dataset_folder(cls, name):
        """The folder of a dataset defined by a catalog: a folder in datasets/ (name matched case-insensitively)
        or a path, which must contain catalog.json; None if there is none."""
        for cand in (join(cls.DATASETS_PATH, name), join(cls.DATASETS_PATH, name.lower()), name):
            if os.path.isfile(join(cand, "catalog.json")):
                return abspath(cand)
        return None

    @classmethod
    def datasets(cls):
        """Names of the datasets defined by a folder in datasets/."""
        if not os.path.isdir(cls.DATASETS_PATH):
            return []
        return sorted(d for d in os.listdir(cls.DATASETS_PATH) if os.path.isfile(join(cls.DATASETS_PATH, d, "catalog.json")))

    @classmethod
    def platforms_of(cls, dataset):
        """The platforms a dataset supports, or None if the dataset does not exist."""
        if dataset in cls.SUPPORTED_PLATFORMS:
            return cls.SUPPORTED_PLATFORMS[dataset]
        return cls.DATASET_PLATFORMS if cls.dataset_folder(dataset) else None

    @classmethod
    def update(cls, **settings):
        """Change settings (e.g. from the command line) and recompute the paths that depend on them."""
        for k, v in settings.items():
            if not hasattr(cls, k):
                raise AttributeError(f"CONFIG has no setting '{k}'")
            setattr(cls, k, v)
        cls._derive()

    @classmethod
    def _derive(cls):
        """Paths and settings computed from the ones above."""
        # DATA GENERATORS HOME
        cls.ABSTRACT_PLAN_GENERATOR = join(cls.PROJECT_PATH, "datafarm", "abstract_generation")
        cls.JOB_GENERATOR = join(cls.PROJECT_PATH, "datafarm", "instantiation")

        # A dataset defined by a folder: the job generator reads its catalog.json, the abstract plans are
        # learned from the input plans its seed queries were turned into (workload/)
        cls.DATASET_FOLDER = None if cls.DATA_MANAGER in cls.INPUT_WORKLOAD else cls.dataset_folder(cls.DATA_MANAGER)
        if cls.DATASET_FOLDER:
            workload = (join(cls.DATASET_FOLDER, "workload"), "", -1, -1)
        else:
            workload = cls.INPUT_WORKLOAD.get(cls.DATA_MANAGER)
            if workload is None:
                raise ValueError(f"Unknown dataset '{cls.DATA_MANAGER}': not one of {', '.join(cls.INPUT_WORKLOAD)} "
                                 f"and no {join(cls.DATASETS_PATH, cls.DATA_MANAGER, 'catalog.json')}")

        # Path to original exec plans
        cls.ORIG_EXEC_PLAN_FOLDER = join(cls.PROJECT_PATH, workload[0])  # <absolute_path_to_input_workload>
        cls.ORIG_EXEC_PLAN_FILTER = workload[1]
        cls.ABSTRACT_PLAN_MAX_DEPTH = workload[2]
        cls.ABSTRACT_PLAN_MAX_JOINS = workload[3]

        # Path of generated data destination
        # Generated output: abstract plans, jobs, execution plans and labels of each experiment
        cls.BASE_PATH = cls.OUTPUT_PATH or join(cls.PROJECT_PATH, "training-data/")

        cls.EXPERIMENT_PATH = join(cls.BASE_PATH, (cls.EXPERIMENT_ID or f"{cls.TARGET_PLATFORM}_{os.path.basename(cls.DATA_MANAGER.rstrip('/'))}") + "/")

        cls.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER = join(
            cls.EXPERIMENT_PATH, "generated_abstract_exec_plans"
        )
        cls.GENERATED_JOB_FOLDER = join(cls.EXPERIMENT_PATH, "generated_jobs")
        cls.GENERATED_JOB_EXEC_PLAN_FOLDER = join(cls.EXPERIMENT_PATH, "generated_jobs_exec_plans")

        cls.GENERATED_JOB_TASK_MANAGER_DETAILS = join(
            cls.EXPERIMENT_PATH, "generated_jobs_task_manager_details"
        )

        # SUBMIT CONF
        # Path where to store the execution plans of the generated jobs after their execution
        cls.GENERATED_JOB_OUTPUT_PLAN_PATH = join(cls.GENERATED_JOB_EXEC_PLAN_FOLDER, cls.DATA_ID + "/")
        cls.GENERATED_JOB_TASK_MANAGER_DETAILS_OUTPUT_PATH = join(
            cls.GENERATED_JOB_TASK_MANAGER_DETAILS, cls.DATA_ID + "/"
        )

        # label forecaster
        cls.GENERATED_METADATA_PATH = join(cls.GENERATED_JOB_FOLDER, "generated_jobs_info.json")
        cls.LABEL_FORECASTER_OUT = join(cls.EXPERIMENT_PATH, "label_forecaster_" + cls.exec_timestamp)

    # Job Submission config
    LOCAL = "nolocal"  # [local|nolocal]
    LOCAL_HEAP = "8GB"  # [8GB|none]
    PARALLELISM = "1"

    # FLINK_TASK_MANAGER_URL
    FLINK_TASK_MANAGER_URL = "http://127.0.0.1:8081/"

    BUILD_SBT = "build.sbt"

    RUN = True

    ########################################################
    ## Config Label Forecaster #############################
    ########################################################

    exec_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    LABEL_COL = "Log_netRunTime"

    SAMPLE_COL = "Log_sourceCardinalitySum"

    FEATURE_COLS = [
        "t_0",
        "t_1",
        "t_2",
        "t_3",
        "t_4",
        "t_5",
        "t_6",
        "joinOutCard_sum",
        "joinOutCard_mean",
        "joinOutCard_std",
        "joinOutCard_min",
        "joinOutCard_max",
        "filterOutCard_mean",
        "filterOutCard_std",
        "filterOutCard_min",
        "filterOutCard_max",
        "groupbyOutCard_mean",
        "groupbyOutCard_std",
        "groupbyOutCard_min",
        "groupbyOutCard_max",
        "outCardinality_mean",
        "outCardinality_std",
        "outCardinality_min",
        "outCardinality_max",
        "outCardinality_kurtosis",
        "outCardinality_skew",
        "sourceOutCard_mean",
        "sourceOutCard_std",
        "sourceOutCard_min",
        "sourceOutCard_max",
        "sourceCardinalitySum",
        "complexity_mean",
        "complexity_min",
        "complexity_max",
    ]

    MAX_EARLY_STOP = 2
    EARLY_STOP_TH = 0.1  # Float between 0 and 1
    MAX_ITER = 5
    INIT_JOBS = 10
    RANDOM_INIT = False
    RANDOM_SAMPLING = False


CONFIG._derive()
