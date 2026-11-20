"""CI check: two dataset folders have the same catalog.json, apart from where and when it was measured."""
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


if catalog(a) != catalog(b):
    sys.exit(f"{a} vs {b}: catalog.json differs (run python -m datafarm.datasets.add_dataset and commit the result)")
print(f"{a}: catalog.json is up to date")
