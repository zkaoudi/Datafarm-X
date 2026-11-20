"""
Measure the statistics of a dataset catalog on the data: table sizes, the selectivity of every filter
candidate and the number of distinct values of the group fields (and of the columns that could be).

Predicates are evaluated with exactly the semantics of the generated jobs (same parsing, same NULL
handling, Java's string order), so a recorded selectivity is the fraction of rows a generated filter keeps.
"""
from datafarm.datasets.csvformat import is_null, java_mod, like, literal, parse, utf16

# when looking for low-cardinality columns, distinct values are counted up to this many per column
DISTINCT_CAP = 100_000


def make_predicate(f, kind):
    """The predicate of a filter candidate over a parsed value (csvformat.parse) of a column of `kind`."""
    op = f["op"].lower()
    v = f.get("value")
    vals = [] if v is None else (v if isinstance(v, list) else [v])
    if kind != "text":
        vals = [literal(kind, x) for x in vals] if op != "mod" else [int(str(x).strip()) for x in vals]
        iv = set(vals)
        nn = lambda x: not is_null(kind, x)
        ops = {
            "=": lambda x: nn(x) and x == vals[0], "!=": lambda x: nn(x) and x != vals[0],
            "<": lambda x: nn(x) and x < vals[0], "<=": lambda x: nn(x) and x <= vals[0],
            ">": lambda x: nn(x) and x > vals[0], ">=": lambda x: nn(x) and x >= vals[0],
            "between": lambda x: nn(x) and vals[0] <= x <= vals[1],
            "in": lambda x: nn(x) and x in iv,
            "is null": lambda x: not nn(x), "is not null": nn,
        }
        if kind == "int":
            ops["mod"] = lambda x: nn(x) and java_mod(x, vals[0]) == vals[1]
    else:
        vals = [str(x) for x in vals]
        k = [utf16(x) for x in vals]
        sv = set(vals)
        ops = {
            "=": lambda x: x is not None and x == vals[0], "!=": lambda x: x is not None and x != vals[0],
            "<": lambda x: x is not None and utf16(x) < k[0], "<=": lambda x: x is not None and utf16(x) <= k[0],
            ">": lambda x: x is not None and utf16(x) > k[0], ">=": lambda x: x is not None and utf16(x) >= k[0],
            "between": lambda x: x is not None and k[0] <= utf16(x) <= k[1],
            "in": lambda x: x in sv,
            "like": lambda x: x is not None and like(x, vals[0]),
            "not like": lambda x: x is not None and not like(x, vals[0]),
            "is null": lambda x: x is None, "is not null": lambda x: x is not None,
        }
    if op not in ops:
        raise ValueError(f"operator '{op}' not supported for a {kind} column")
    return ops[op]


def analyze_table(path, table, fmt, distinct_columns, log=print):
    """One pass over a table file -> (rows, [selectivity per filter], {column: distinct count or None if > cap}).

    `distinct_columns`: column -> cap: the columns whose distinct values are counted, exactly (cap None) or
    until more than `cap` are seen (then None is returned for the column). Text NULLs count as "" (the generated
    group-by keys map NULL text to ""), integer NULLs as one value."""
    cols = table["columns"]
    n = len(cols)
    idx = {c["name"]: i for i, c in enumerate(cols)}
    preds = []
    for f in table.get("filters", []):
        i = idx[f["field"]]
        kind = cols[i]["type"]
        preds.append((i, kind, make_predicate(f, kind)))
    groups = [(idx[c], cols[idx[c]]["type"], set(), cap) for c, cap in distinct_columns.items()]
    capped = set()

    rows = 0
    hits = [0] * len(preds)
    with open(path, encoding=fmt.encoding, errors="replace", newline=None) as fh:
        first = True
        for line in fh:
            if line.endswith("\n"):
                line = line[:-1]
            if first and fmt.header:
                first = False
                continue
            first = False
            raw = fmt.split(line, n)
            rows += 1
            for k, (i, kind, pred) in enumerate(preds):
                if pred(parse(kind, raw[i])):
                    hits[k] += 1
            for i, kind, seen, cap in groups:
                if i in capped:
                    continue
                v = parse(kind, raw[i])
                # the generated group-by keys: NULL text is "", a NULL decimal (NaN) one key like any NULL
                seen.add("" if v is None else None if kind == "decimal" and v != v else v)
                if cap is not None and len(seen) > cap:
                    capped.add(i)
                    seen.clear()
            if rows % 5_000_000 == 0:
                log(f"|   {table['name']}: {rows:,} rows")
    distinct = {cols[i]["name"]: (None if i in capped else len(seen)) for i, _, seen, _ in groups}
    return rows, [h / rows if rows else 0.0 for h in hits], distinct
