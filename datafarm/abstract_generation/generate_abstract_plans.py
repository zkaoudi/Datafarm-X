import os
import sys

# also runnable as a script: put the repository root on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from datafarm.abstract_generation import ExecutionPlanTools as planTools  # noqa: E402
from datafarm.abstract_generation.AbstractPlan import AbstactPlanGeneratorModel  # noqa: E402


def parse_args(args):
    params = {}
    if 4 <= args.__len__() <= 7:
        params["nJobs"] = int(args[1])
        params["originalExecPlanSource"] = args[2]
        params["genAbsPlanDest"] = args[3]
        # only plan files whose name contains this string are used
        params["planFilter"] = args[4] if args.__len__() > 4 else "TPC_H"
        # -1 = learned from the input workload
        params["maxDepth"] = int(args[5]) if args.__len__() > 5 else 6
        params["maxJoins"] = int(args[6]) if args.__len__() > 6 else 3
    else:
        raise Exception(
            "Expected arguments: <nJobs> <originalExecPlanSource> <genAbsPlanDest> [planFilter] [maxDepth] [maxJoins]"
        )
    return params


if __name__ == "__main__":
    params = parse_args(sys.argv)

    if not os.path.exists(params["genAbsPlanDest"]):
        raise Exception(
            f"Destination folder for genAbsPlanDest '{params['genAbsPlanDest']}' does not exists."
        )

    exec_plans_json = planTools.load_exec_plans(params["originalExecPlanSource"])

    exec_plans_json = [ep for ep in exec_plans_json if (params["planFilter"] in ep[0])]
    if not exec_plans_json:
        raise Exception(
            f"No plans matching '{params['planFilter']}' in '{params['originalExecPlanSource']}'"
        )

    print(f"Learning from {len(exec_plans_json)} input plans; generating {params['nJobs']} abstract plans")

    exec_plans_graph = planTools.compute_graphs_from_plans(
        exec_plans_json, include_cycles=False
    )

    absModel = AbstactPlanGeneratorModel(
        max_depth=params["maxDepth"], max_joins=params["maxJoins"], seed=42
    )

    absModel.fit(exec_plans_graph)

    generatedAbstactractPlans = absModel.predict(params["nJobs"])

    for pg in generatedAbstactractPlans:
        # planTools.show_generated_exec_plan_graph(pg)
        planTools.save_generated_exec_plan_graph(
            pg, params["genAbsPlanDest"], save_graph_plot=True
        )
