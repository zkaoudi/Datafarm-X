"""CI check: two dataset folders have the same catalog.json (apart from where and when it was measured) and workload."""
import glob
import json
import os
import sys

a, b = sys.argv[1], sys.argv[2]


def catalog(folder):
    with open(os.path.join(folder, "catalog.json"), encoding="utf-8") as fh:
        c = json.load(fh)
    c.pop("measuredOn", None)
    c.pop("name", None)
    return c


def workload(folder):
    out = {}
    for f in sorted(glob.glob(os.path.join(folder, "workload", "*.json"))):
        with open(f, encoding="utf-8") as fh:
            p = json.load(fh)
        p.pop("source", None)
        out[os.path.basename(f)] = p
    return out


problems = []
if catalog(a) != catalog(b):
    problems.append("catalog.json differs")
if workload(a) != workload(b):
    problems.append("workload/ differs")
if problems:
    sys.exit(f"{a} vs {b}: " + ", ".join(problems) + " (run python -m datafarm.datasets.add_dataset and commit the result)")
print(f"{a}: catalog.json and workload/ are up to date")
