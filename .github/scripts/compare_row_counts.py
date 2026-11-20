"""CI check: every generated job returns the same number of rows on every platform.

Takes the results CSVs of the runners in datafarm/execution (columns job, status, rows) as LABEL=PATH
and fails if a job failed anywhere or if its row counts differ.

    python .github/scripts/compare_row_counts.py spark=spark.csv wayang-java=wayang_java.csv sql-duckdb=duckdb.csv
"""
import argparse
import csv
import re
import sys


def read_results(path):
    with open(path, newline="") as fh:
        return {r["job"]: (int(r["rows"]) if r["status"] == "ok" else r["status"]) for r in csv.DictReader(fh)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", nargs="+", metavar="LABEL=CSV", help="results CSV of a runner, with a label")
    args = ap.parse_args()

    platforms = {}
    for item in args.results:
        label, _, path = item.partition("=")
        platforms[label] = read_results(path)
    labels = list(platforms)
    jobs = sorted(set().union(*platforms.values()), key=lambda n: [int(x) for x in re.findall(r"\d+", n)])
    print("job      " + "".join(f"{l:>14}" for l in labels))
    bad = []
    for job in jobs:
        values = [platforms[l].get(job, "missing") for l in labels]
        print(f"{job:<9}" + "".join(f"{str(v):>14}" for v in values))
        if any(not isinstance(v, int) for v in values) or len(set(values)) > 1:
            bad.append(job)
    if bad:
        print(f"{len(bad)} of {len(jobs)} jobs differ between platforms or failed: {', '.join(bad)}")
        sys.exit(1)
    print(f"{len(jobs)} jobs return the same number of rows on {', '.join(labels)}")


if __name__ == "__main__":
    main()
