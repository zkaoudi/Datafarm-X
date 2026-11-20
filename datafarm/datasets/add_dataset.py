"""
Add a dataset to DataFarm: derive its catalog from a schema, seed queries and the data.

A dataset is a folder datasets/<name>/ with
    schema.sql      CREATE TABLE statements: column types, NOT NULL, PRIMARY KEY, FOREIGN KEY/REFERENCES
    queries/*.sql   seed queries (select-project-join with filters and aggregates), or --queries DIR
    dataset.json    optional: file format and overrides (see datasets/README.md)

This writes
    catalog.json    tables, columns, join graph, filter candidates, group fields and statistics,
                    which the job generator reads (edit it, or better dataset.json, to adjust)

The dataset says what the data is; the input workload, from which DataFarm learns the shape of the jobs, is
chosen separately (RunGenerator.py --workload). By default it is input-workloads/<name>/ or, if there is none,
the seed queries themselves.

The join graph comes from the equi-joins of the queries plus the foreign keys of schema.sql, the filter
candidates from the queries' predicates, and with --data the statistics from the data files (one pass per
table): row counts, the selectivity of every filter and the distinct values of the group fields.

Usage:
    python -m datafarm.datasets.add_dataset datasets/<name> [--data /path/to/files] [--queries DIR]
    python RunGenerator.py --dataset <name> --platform Spark
"""
import argparse
import datetime
import glob
import json
import os
import re
import sys
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # repository root
from datafarm.datasets import analyze, queries as Q  # noqa: E402
from datafarm.datasets.csvformat import Format  # noqa: E402
from datafarm.datasets.schema import parse_schema  # noqa: E402

SETTINGS = {"about", "format", "files", "groupFields", "joins", "maxOccurrences", "exclude", "filters"}
MOD_FILTERS = [[2, 0], [4, 0], [10, 0]]
MAX_GROUP_FIELDS = 4


def natural_key(path):
    name = os.path.basename(path)
    return [int(x) if x.isdigit() else x for x in re.split(r"(\d+)", name)]


def load_settings(folder):
    path = os.path.join(folder, "dataset.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        s = json.load(fh)
    unknown = set(s) - SETTINGS
    if unknown:
        raise SystemExit(f"{path}: unknown settings {', '.join(sorted(unknown))} (known: {', '.join(sorted(SETTINGS))})")
    return s


def read_queries(qdir, tables, log):
    """-> [(name, parsed query, {alias: [conjunct]}, [join], [(table, filter)])], skipped reasons."""
    files = sorted(glob.glob(os.path.join(qdir, "*.sql")), key=natural_key) if qdir and os.path.isdir(qdir) else []
    columns_of = {t["name"]: OrderedDict((c["name"], c) for c in t["columns"]) for t in tables}
    out, notes = [], []
    for f in files:
        name = os.path.basename(f)[:-4]
        try:
            with open(f, encoding="utf-8") as fh:
                q = Q.parse_query(fh.read(), list(columns_of))
        except Q.UnsupportedQuery as e:
            notes.append(f"query {name} skipped: {e}")
            continue
        q["name"] = name
        alias_tables = {a: t for t, a in q["tables"]}
        filters = {a: [] for a in alias_tables}
        joins, preds = [], []
        for c in q["conjuncts"]:
            kind = Q.classify(c, alias_tables, columns_of)
            if kind[0] == "join":
                (a, x), (b, y) = kind[1], kind[2]
                if x not in columns_of[alias_tables[a]] or y not in columns_of[alias_tables[b]]:
                    notes.append(f"query {name}: unknown join column in {c}")
                    continue
                joins.append(((a, x), (b, y)))
            elif kind[0] == "filter":
                filters[kind[1]].append(c)
                if not kind[2]:
                    notes.append(f"query {name}: {kind[3]}")
                preds += [(alias_tables[kind[1]], f) for f in kind[2]]
            else:
                notes.append(f"query {name}: {kind[1]} (ignored)")
        try:
            Q.join_order(q["tables"], joins)
        except Q.UnsupportedQuery as e:
            notes.append(f"query {name} skipped: {e}")
            continue
        out.append((name, q, filters, joins, preds))
    return out, notes


def build_catalog(name, tables, settings, parsed, log):
    by_name = {t["name"]: t for t in tables}
    col = lambda t, c: next((x for x in by_name[t]["columns"] if x["name"] == c), None)

    # ---- join graph: query equi-joins (most frequent first), extra joins of dataset.json, foreign keys
    edges = OrderedDict()

    def add_edge(a, b, n, origin):
        (t1, c1), (t2, c2) = a, b
        if t1 not in by_name or t2 not in by_name:
            return
        if t1 == t2:
            log(f"| note: self-join {t1}.{c1} = {t2}.{c2} ({origin}) is not used: joins are between different tables")
            return
        x, y = col(t1, c1), col(t2, c2)
        if x is None or y is None:
            raise SystemExit(f"join {t1}.{c1} = {t2}.{c2} ({origin}): unknown column")
        if x["type"] != y["type"]:
            log(f"| note: join {t1}.{c1} = {t2}.{c2} ({origin}) is not used: {x['type']} and {y['type']} columns")
            return
        key = frozenset([(t1, c1), (t2, c2)])
        if key in edges:
            edges[key]["queries"] += n
        else:
            edges[key] = {"left": f"{t1}.{c1}", "right": f"{t2}.{c2}", "queries": n}

    for qname, q, _, joins, _ in parsed:
        at = {a: t for t, a in q["tables"]}
        for (a, x), (b, y) in joins:
            add_edge((at[a], x), (at[b], y), 1, f"query {qname}")
    order = list(edges)
    edges = OrderedDict(sorted(edges.items(), key=lambda kv: (-kv[1]["queries"], order.index(kv[0]))))
    for j in settings.get("joins", []):
        a, b = [tuple(s.split(".")) for s in j]
        add_edge(a, b, 0, "dataset.json")
    for t in tables:
        for fk in t["foreignKeys"]:
            for c, rc in zip(fk["columns"], fk["refColumns"]):
                add_edge((t["name"], c), (fk["table"], rc), 0, "foreign key")

    joined = {}
    for e in edges.values():
        for side in ("left", "right"):
            t, c = e[side].split(".")
            joined.setdefault(t, []).append(c)
    unjoined = [t["name"] for t in tables if t["name"] not in joined]
    if unjoined:
        log(f"| note: no join with {', '.join(unjoined)}: generated jobs never read these tables")

    # ---- filter candidates: those of dataset.json first, then the seed queries' predicates
    filters = {t["name"]: OrderedDict() for t in tables}
    for tn, fs in settings.get("filters", {}).items():
        if tn not in by_name:
            raise SystemExit(f"dataset.json filters: unknown table {tn}")
        cols = OrderedDict((c["name"], c) for c in by_name[tn]["columns"])
        for f in fs:
            if set(f) - {"field", "op", "value"} or "field" not in f or "op" not in f:
                raise SystemExit(f"dataset.json filters of {tn}: expected {{field, op, value}}, not {f}")
            if f["field"] not in cols:
                raise SystemExit(f"dataset.json filters of {tn}: unknown column {f['field']}")
            try:
                analyze.make_predicate(f, cols[f["field"]]["type"])  # checks operator and constants
            except (ValueError, IndexError) as e:
                raise SystemExit(f"dataset.json filters of {tn}: {f}: {e}")
            f = {"field": f["field"], "op": f["op"].lower(), "value": f.get("value")}
            filters[tn][(f["field"], f["op"], json.dumps(f["value"]))] = dict(f, selectivity=None, queries=0)
    for _, _, _, _, preds in parsed:
        for t, f in preds:
            key = (f["field"], f["op"], json.dumps(f["value"]))
            if key in filters[t]:
                filters[t][key]["queries"] += 1
            else:
                filters[t][key] = dict(f, selectivity=None, queries=1)

    # ---- table occurrences per query
    max_occ = {t["name"]: 1 for t in tables}
    for _, q, _, _, _ in parsed:
        for t in {t for t, _ in q["tables"]}:
            max_occ[t] = max(max_occ[t], sum(1 for x, _ in q["tables"] if x == t))
    max_occ.update(settings.get("maxOccurrences", {}))

    fmt = Format.from_json(settings.get("format"))
    files = settings.get("files", {})
    out_tables = []
    for t in tables:
        tn = t["name"]
        fs = list(filters[tn].values())
        if not fs:
            # tables the queries never filter: a filter that keeps about 1/m of the rows of a dense integer key
            key = next((c for c in t["columns"] if c["type"] == "int" and not c["nullable"]), None)
            if key is not None:
                fs = [{"field": key["name"], "op": "mod", "value": v, "selectivity": None, "queries": 0}
                      for v in MOD_FILTERS]
            else:
                c = t["columns"][0]
                fs = [{"field": c["name"], "op": "is not null", "value": None, "selectivity": None, "queries": 0}]
        out_tables.append(OrderedDict([
            ("name", tn),
            ("file", files.get(tn, tn + fmt.extension)),
            ("header", None),
            ("rows", None),
            ("columns", [OrderedDict([("name", c["name"]), ("type", c["type"]), ("nullable", c["nullable"]),
                                      ("sqlType", c["sqlType"])] + ([] if c["ordered"] else [("ordered", False)]))
                         for c in t["columns"]]),
            ("primaryKey", t["primaryKey"]),
            ("groupFields", [{"field": g, "distinct": None}
                             for g in settings.get("groupFields", {}).get(tn, [])]),
            ("filters", fs),
            ("maxOccurrences", max_occ[tn]),
        ]))
        if len(t["columns"]) > 22:
            raise SystemExit(f"table {tn} has {len(t['columns'])} columns; the generated jobs hold rows in Scala "
                             "tuples, which have at most 22 fields")
    return OrderedDict([
        ("name", name),
        ("about", settings.get("about", f"Dataset {name}, added with datafarm/datasets/add_dataset.py.")),
        ("format", fmt.to_json()),
        ("measuredOn", None),
        ("tables", out_tables),
        ("joins", list(edges.values())),
    ]), joined


def choose_group_fields(table, join_columns, distinct, overridden):
    """Group fields when dataset.json gives none: the join columns that are not the primary key (the keys of the
    relationships, as in JOB), then low-cardinality columns found in the data; else the first non-key column."""
    if overridden:
        return [g["field"] for g in table["groupFields"]]
    pk = set(table["primaryKey"]) if len(table["primaryKey"]) == 1 else set()
    names = [c["name"] for c in table["columns"]]
    picks = [c for c in names if c in join_columns and c not in pk]
    rows = table["rows"] or 0
    low = sorted((d, names.index(c), c) for c, d in distinct.items()
                 if d is not None and 2 <= d <= rows // 10 and c not in pk and c not in picks)
    picks += [c for _, _, c in low]
    if not picks:
        picks = [c for c in names if c not in pk][:1] or names[:1]
    return picks[:MAX_GROUP_FIELDS]


def measure(catalog, data_dir, joined, log):
    fmt = Format.from_json(catalog["format"])
    for t in catalog["tables"]:
        path = os.path.join(data_dir, t["file"])
        if not os.path.exists(path):
            raise SystemExit(f"Missing data file {path}")
    for t in catalog["tables"]:
        path = os.path.join(data_dir, t["file"])
        overridden = bool(t["groupFields"])
        pk = set(t["primaryKey"]) if len(t["primaryKey"]) == 1 else set()
        if overridden:
            counted = {g["field"]: None for g in t["groupFields"]}
        else:
            # join columns exactly, the others until they turn out to have many values
            counted = {c["name"]: (None if c["name"] in joined.get(t["name"], []) else analyze.DISTINCT_CAP)
                       for c in t["columns"] if c["name"] not in pk}
        log(f"| Analyzing {t['name']} ({path})")
        rows, sels, distinct = analyze.analyze_table(path, t, fmt, counted, log)
        t["rows"] = rows
        if fmt.header:
            t["header"] = fmt.header_line(path)
        for f, s in zip(t["filters"], sels):
            f["selectivity"] = round(s, 8)
        groups = choose_group_fields(t, joined.get(t["name"], []), distinct, overridden)
        t["groupFields"] = [{"field": g, "distinct": distinct.get(g)} for g in groups]
        log(f"|   {rows:,} rows; group fields " + ", ".join(f"{g['field']} ({g['distinct']})" for g in t["groupFields"]))
    catalog["measuredOn"] = {"dataDir": data_dir, "date": datetime.date.today().isoformat()}


def check(catalog):
    """Consistency checks the job generator relies on."""
    for t in catalog["tables"]:
        names = [c["name"] for c in t["columns"]]
        for g in t["groupFields"]:
            if g["field"] not in names:
                raise SystemExit(f"group field {t['name']}.{g['field']} is not a column")
        if not t["groupFields"]:
            raise SystemExit(f"table {t['name']} has no group field")
        if not t["filters"]:
            raise SystemExit(f"table {t['name']} has no filter candidate")
        if fmt_header_missing(catalog, t):
            raise SystemExit(f"format.header is set, but the header of {t['file']} is unknown: run with --data")


def fmt_header_missing(catalog, t):
    return catalog["format"]["header"] and t["header"] is None


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folder", help="dataset folder with schema.sql (and queries/, dataset.json)")
    ap.add_argument("--data", help="folder with the data files, to measure statistics")
    ap.add_argument("--queries", help="folder with the seed queries (default: <folder>/queries)")
    ap.add_argument("--name", help="dataset name (default: the folder name)")
    args = ap.parse_args(argv)
    log = print

    folder = args.folder.rstrip("/")
    name = args.name or os.path.basename(os.path.abspath(folder))
    schema_path = os.path.join(folder, "schema.sql")
    if not os.path.exists(schema_path):
        raise SystemExit(f"No {schema_path}")
    settings = load_settings(folder)
    with open(schema_path, encoding="utf-8") as fh:
        tables = parse_schema(fh.read())
    excluded = set(settings.get("exclude", []))
    tables = [t for t in tables if t["name"] not in excluded]
    log(f"| {len(tables)} tables in {schema_path}")

    qdir = args.queries or os.path.join(folder, "queries")
    parsed, notes = read_queries(qdir, tables, log)
    for n in notes:
        log("| note: " + n)
    log(f"| {len(parsed)} seed queries from {qdir}" + ("" if parsed else " (none)"))

    catalog, joined = build_catalog(name, tables, settings, parsed, log)
    if args.data:
        measure(catalog, args.data, joined, log)
    else:
        for t in catalog["tables"]:
            if not t["groupFields"]:
                t["groupFields"] = [{"field": g, "distinct": None}
                                    for g in choose_group_fields(t, joined.get(t["name"], []), {}, False)]
        log("| no --data: statistics stay null (recorded as -1 in generated_jobs_info.json)")
    check(catalog)

    with open(os.path.join(folder, "catalog.json"), "w", encoding="utf-8") as fh:
        json.dump(catalog, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    log(f"| wrote {os.path.join(folder, 'catalog.json')}: {len(catalog['joins'])} join edges, "
        f"{sum(len(t['filters']) for t in catalog['tables'])} filter candidates")

    log(f"| done: python RunGenerator.py --dataset {name} --platform <Flink|Wayang|Spark|SQL|SQLServer>")


if __name__ == "__main__":
    main()
