"""
Turn a workload of SQL queries into the plans DataFarm learns the shape of its jobs from.

An input workload is either Flink execution plans (*.json, as Flink writes them) or SQL queries (*.sql).
DataFarm learns from plans, so SQL queries are translated, with SQL's semantics, into the plan of the
Flink DataSet job one would write for each query (see datafarm/datasets/queries.py):

    per table:   Data Source -> Filter                    (Filter only if the query filters that table)
    joins:       left-deep chain of Join operators        (no cross products)
    aggregate:   Reduce -> Reduce, driver "Reduce All"    (aggregates without GROUP BY: one row)
                 GroupCombine -> GroupReduce              (GROUP BY or DISTINCT: one row per key)
    order:       Sort-Partition                           (ORDER BY)
    output:      Data Sink

The queries are read against the tables of a dataset (its catalog.json). RunGenerator.py does this by
itself when --workload is a folder of SQL queries; run this to keep the plans, e.g. when the queries can
not be shared:

    python -m datafarm.datasets.workload /path/to/queries --dataset job --out input-workloads/job
"""
import argparse
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # repository root
from datafarm.datasets import catalog as catalogs  # noqa: E402
from datafarm.datasets import queries as Q  # noqa: E402
from datafarm.datasets.add_dataset import read_queries  # noqa: E402


def write_plans(query_dir, dataset, out_dir, log=print):
    """Translates the *.sql files of query_dir into plans in out_dir (replacing its *.json); returns their number."""
    cat = catalogs.load(dataset)
    parsed, notes = read_queries(query_dir, cat["tables"], log)
    for n in notes:
        log("| note: " + n)
    if not parsed:
        raise SystemExit(f"No query of {query_dir} could be read against dataset {cat['name']}")
    os.makedirs(out_dir, exist_ok=True)
    for old in glob.glob(os.path.join(out_dir, "*.json")):
        os.remove(old)
    for qname, q, filters, joins, _ in parsed:
        plan = Q.build_plan(q, filters, joins, f"Translated from SQL query {qname} over dataset {cat['name']} by "
                                               "datafarm/datasets/workload.py (operator graph only; not produced "
                                               "by Flink).")
        with open(os.path.join(out_dir, f"{qname}.json"), "w", encoding="utf-8") as fh:
            json.dump(plan, fh, indent=1)
            fh.write("\n")
    log(f"| {len(parsed)} plans from the queries of {query_dir} in {out_dir}")
    return len(parsed)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("queries", help="folder with the SQL queries (*.sql)")
    ap.add_argument("--dataset", required=True, help="dataset the queries are over: its name in datasets/ or its folder")
    ap.add_argument("--out", required=True, help="folder for the plans")
    args = ap.parse_args(argv)
    write_plans(args.queries, args.dataset, args.out)


if __name__ == "__main__":
    main()
