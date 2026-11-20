"""CI check: two folders hold the same input plans, apart from their "source" description."""
import glob
import json
import os
import sys

a, b = sys.argv[1], sys.argv[2]


def plans(folder):
    out = {}
    for f in sorted(glob.glob(os.path.join(folder, "*.json"))):
        with open(f, encoding="utf-8") as fh:
            p = json.load(fh)
        p.pop("source", None)
        out[os.path.basename(f)] = p
    return out


pa, pb = plans(a), plans(b)
if pa != pb:
    diff = sorted(set(pa) ^ set(pb)) + sorted(k for k in set(pa) & set(pb) if pa[k] != pb[k])
    sys.exit(f"{a} vs {b}: plans differ: {', '.join(diff[:10])}")
print(f"{a}: {len(pa)} plans are up to date")
