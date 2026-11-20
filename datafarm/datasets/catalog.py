"""Find and read the catalog.json of a dataset defined by a folder (see datafarm/datasets/add_dataset.py)."""
import json
import os

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATASETS = os.path.join(REPO, "datasets")


def find(dataset):
    """Path of the catalog.json of `dataset`: a folder name in datasets/ (case-insensitive), a folder or the file."""
    for cand in (os.path.join(DATASETS, dataset), os.path.join(DATASETS, dataset.lower()), dataset):
        if os.path.isfile(cand) and cand.endswith(".json"):
            return cand
        if os.path.isfile(os.path.join(cand, "catalog.json")):
            return os.path.join(cand, "catalog.json")
    return None


def load(dataset):
    path = find(dataset)
    if path is None:
        raise SystemExit(f"Unknown dataset '{dataset}': no {os.path.join(DATASETS, dataset, 'catalog.json')} "
                         "(create it with: python -m datafarm.datasets.add_dataset datasets/<name>)")
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)
