"""
Read a dataset's seed queries: which tables they scan, filter and join, and how they aggregate.

Supported: SELECT ... FROM t1 [AS] a, t2 [AS] b, ... [JOIN t3 [AS] c ON ...] WHERE ... [GROUP BY ...]
[HAVING ...] [ORDER BY ...] [LIMIT n], i.e. select-project-join queries with filters, equi-joins and
aggregates. Subqueries, UNION, outer joins and the like are not; such queries are skipped with a message.

From each query:
  - every WHERE (and ON) conjunct is an equi-join `a.x = b.y` between two tables or a filter on one table;
  - the filters that DataFarm can generate (comparisons, BETWEEN, IN, LIKE, NOT LIKE, IS [NOT] NULL with
    constants) become filter candidates of their column; others (OR, functions, ...) are reported;
  - the query becomes an input plan for learning the shape of the generated jobs, written like a
    hand-coded Flink DataSet job for the query:

        per table:   Data Source -> Filter                 (Filter only if the query filters that table)
        joins:       left-deep chain of Join operators     (no cross products)
        aggregate:   Reduce -> Reduce                      (aggregates without GROUP BY: combiner + reduce)
                     GroupCombine -> GroupReduce           (GROUP BY or DISTINCT)
        order:       Sort-Partition                        (ORDER BY)
        output:      Data Sink
"""
import re


class UnsupportedQuery(ValueError):
    pass


def strip_sql(sql):
    s = re.sub(r"--[^\n]*", " ", sql)
    s = re.sub(r"/\*.*?\*/", " ", s, flags=re.S)
    return re.sub(r"\s+", " ", s).strip().rstrip(";").strip()


def split_conjuncts(where, word="AND"):
    """Split a WHERE clause on top-level AND (outside parentheses and quotes); keeps BETWEEN x AND y.
    With word="OR", splits a disjunction instead."""
    parts, buf, depth, in_quote = [], [], 0, False
    n = len(word)
    i = 0
    while i < len(where):
        c = where[i]
        if c == "'":
            in_quote = not in_quote
        if not in_quote:
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif (depth == 0 and where[i:i + n].upper() == word
                  and (i == 0 or not where[i - 1].isalnum())
                  and (i + n >= len(where) or not where[i + n].isalnum())):
                prev = "".join(buf)
                if word != "AND" or not re.search(r"\bBETWEEN\s+(?:(?:DATE|TIMESTAMP)\s+)?(?:'(?:[^']|'')*'|\S+)\s*$",
                                                  prev, re.I):
                    parts.append(prev.strip())
                    buf = []
                    i += n
                    continue
        buf.append(c)
        i += 1
    parts.append("".join(buf).strip())
    return [p for p in parts if p]


# lookahead for the end of a clause: the next clause keyword or the end of the query
_END = r"(?=\s+(?:WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|OFFSET|FETCH)\b|$)"
_RESERVED = {"on", "join", "inner", "where", "group", "order", "limit", "having", "cross", "natural", "left",
             "right", "full", "outer", "using"}


def _clause(s, start_kw):
    m = re.search(r"\b" + start_kw + r"\b(.*?)" + _END, s, re.I)
    return m.group(1).strip() if m else ""


def parse_query(sql, table_names):
    """-> dict(tables [(table, alias)], conjuncts [str], select str, group_by bool, distinct bool, order_by bool)."""
    s = strip_sql(sql)
    if re.search(r"\(\s*SELECT\b", s, re.I) or re.search(r"\b(UNION|INTERSECT|EXCEPT|WITH)\b", s, re.I):
        raise UnsupportedQuery("subqueries, WITH and set operations are not supported")
    m = re.match(r"SELECT\s+(DISTINCT\s+)?(.*?)\s+FROM\s+(.*)$", s, re.I)
    if not m:
        raise UnsupportedQuery("expected SELECT ... FROM ...")
    distinct, select, rest = bool(m.group(1)), m.group(2), m.group(3)
    frm = re.match(r"(.*?)" + _END, rest, re.I).group(1).strip()
    where = _clause(rest, "WHERE")
    group_by = bool(_clause(rest, r"GROUP\s+BY"))
    order_by = bool(_clause(rest, r"ORDER\s+BY"))

    if re.search(r"\b(LEFT|RIGHT|FULL|OUTER|CROSS|NATURAL)\b|\bUSING\s*\(", frm, re.I):
        raise UnsupportedQuery("only inner joins are supported")
    conjuncts = split_conjuncts(where) if where else []
    # explicit joins: "a JOIN b ON cond" -> FROM items a, b and cond as a conjunct
    items = []
    for part in re.split(r",", frm):
        pieces = re.split(r"\b(?:INNER\s+)?JOIN\b", part, flags=re.I)
        for k, piece in enumerate(pieces):
            on = re.split(r"\bON\b", piece, maxsplit=1, flags=re.I)
            items.append(on[0].strip())
            if len(on) > 1:
                if k == 0:
                    raise UnsupportedQuery(f"ON without JOIN: {piece}")
                conjuncts += split_conjuncts(on[1].strip())
    tables = []
    known = {t.lower(): t for t in table_names}
    for it in items:
        tm = re.fullmatch(r"([\w.\"]+)(?:\s+(?:AS\s+)?(\w+))?", it, re.I)
        if not tm:
            raise UnsupportedQuery(f"can not read the FROM item '{it}'")
        t = tm.group(1).split(".")[-1].strip('"').lower()
        if t not in known:
            raise UnsupportedQuery(f"unknown table '{t}'")
        alias = tm.group(2) if tm.group(2) and tm.group(2).lower() not in _RESERVED else known[t]
        tables.append((known[t], alias))
    aliases = [a for _, a in tables]
    if len(set(aliases)) != len(aliases):
        raise UnsupportedQuery("the same table name or alias appears twice")
    return {"tables": tables, "conjuncts": conjuncts, "select": select.strip(), "group_by": group_by,
            "distinct": distinct, "order_by": order_by}


# ---- predicates ---------------------------------------------------------------------------------

_REF = r"(?:(\w+)\.)?(\w+)"
_STR = r"'(?:[^']|'')*'"
_NUM = r"[+-]?\d+(?:\.\d+)?"
_LIT = r"(?:(?:DATE|TIMESTAMP)\s+" + _STR + "|" + _STR + "|" + _NUM + ")"
_OPS = r"(<>|!=|<=|>=|=|<|>)"
_FLIP = {"<": ">", ">": "<", "<=": ">=", ">=": "<=", "=": "=", "!=": "!=", "<>": "<>"}


def _lit(s):
    """SQL literal -> (python value as str, is_number)."""
    s = s.strip()
    m = re.fullmatch(r"(?:DATE|TIMESTAMP)\s+('(?:[^']|'')*')", s, re.I)
    if m:
        s = m.group(1)
    if s.startswith("'"):
        return s[1:-1].replace("''", "'"), False
    return s, True


def _unparen(c):
    c = c.strip()
    while c.startswith("(") and c.endswith(")"):
        depth = 0
        for i, ch in enumerate(c):
            depth += {"(": 1, ")": -1}.get(ch, 0)
            if depth == 0 and i < len(c) - 1:
                return c
        c = c[1:-1].strip()
    return c


def refs(conjunct, aliases):
    """Aliases (or table names) referenced as alias.column in a conjunct (outside string literals)."""
    return {a for a in re.findall(r"\b(\w+)\.\w+", re.sub(_STR, "''", conjunct)) if a in aliases}


def classify(conjunct, alias_tables, columns_of):
    """A WHERE conjunct -> ("join", (alias, col), (alias, col)) | ("filter", alias, [filter], reason)
    | ("other", reason). `alias_tables`: alias -> table; `columns_of`: table -> {column: column dict}.

    A disjunction of supported filters on one table (`x LIKE 'a%' OR x LIKE 'b%'`) gives each disjunct
    as a filter candidate."""
    c = _unparen(conjunct)
    aliases = set(alias_tables)
    j = re.fullmatch(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", c)
    if j and j.group(1) in aliases and j.group(3) in aliases and j.group(1) != j.group(3):
        return ("join", (j.group(1), j.group(2).lower()), (j.group(3), j.group(4).lower()))
    rs = refs(c, aliases)
    if not rs:
        # bare column names: resolve by the single table that has that column
        names = set(re.findall(r"\b([A-Za-z_]\w*)\b", re.sub(_STR, "''", c)))
        owners = {a for a, t in alias_tables.items() if names & set(columns_of[t])}
        if len(owners) == 1:
            rs = owners
    if len(rs) != 1:
        return ("other", f"predicate over {sorted(rs) or 'no table'} is neither a filter nor an equi-join: {conjunct}")
    alias = rs.pop()
    cols = columns_of[alias_tables[alias]]
    found = []
    for d in split_conjuncts(c, "OR"):
        f, reason = parse_filter(_unparen(d), alias, cols)
        if f is None:
            return ("filter", alias, [], reason)
        found.append(f)
    return ("filter", alias, found, None)


def parse_filter(c, alias, columns):
    """A one-table predicate -> (filter dict {field, op, value} or None, reason if None)."""
    def col(a, name):
        if a is not None and a != alias:
            return None
        return columns.get(name.lower())

    pats = [
        ("cmp", r"" + _REF + r"\s*" + _OPS + r"\s*(" + _LIT + ")"),
        ("rcmp", r"(" + _LIT + r")\s*" + _OPS + r"\s*" + _REF),
        ("between", _REF + r"\s+(NOT\s+)?BETWEEN\s+(" + _LIT + r")\s+AND\s+(" + _LIT + ")"),
        ("in", _REF + r"\s+(NOT\s+)?IN\s*\(\s*(" + _LIT + r"(?:\s*,\s*" + _LIT + r")*)\s*\)"),
        ("like", _REF + r"\s+(NOT\s+)?LIKE\s+(" + _STR + ")"),
        ("null", _REF + r"\s+IS\s+(NOT\s+)?NULL"),
    ]
    for kind, p in pats:
        m = re.fullmatch(p, c, re.I | re.S)
        if not m:
            continue
        g = m.groups()
        if kind == "cmp":
            column, op, vals = col(g[0], g[1]), g[2], [g[3]]
        elif kind == "rcmp":
            column, op, vals = col(g[2], g[3]), _FLIP[g[1]], [g[0]]
        elif kind == "between":
            if g[2]:
                return None, f"NOT BETWEEN is not supported: {c}"
            column, op, vals = col(g[0], g[1]), "between", [g[3], g[4]]
        elif kind == "in":
            if g[2]:
                return None, f"NOT IN is not supported: {c}"
            column, op = col(g[0], g[1]), "in"
            vals = re.findall(_LIT, g[3], re.I)
        elif kind == "like":
            column, op, vals = col(g[0], g[1]), "not like" if g[2] else "like", [g[3]]
        else:
            column, op, vals = col(g[0], g[1]), "is not null" if g[2] else "is null", []
        if column is None:
            return None, f"unknown column in {c}"
        op = "!=" if op == "<>" else op
        parsed = [_lit(v) for v in vals]
        if column["type"] == "int":
            if op in ("like", "not like"):
                return None, f"LIKE on an integer column: {c}"
            if any(not num or not re.fullmatch(r"[+-]?\d+", v) for v, num in parsed):
                return None, f"non-integer constant for an integer column: {c}"
            values = [int(v) for v, _ in parsed]
        else:
            if not column.get("ordered", True) and op not in ("is null", "is not null"):
                return None, f"column {column['name']} ({column['sqlType']}) is read as text in this version: {c}"
            if any(num for _, num in parsed):
                return None, f"number compared with a text column: {c}"
            values = [v for v, _ in parsed]
        value = None if not values else values[0] if op not in ("between", "in") else values
        return {"field": column["name"], "op": op, "value": value}, None
    return None, f"not a supported filter: {c}"


# ---- plans --------------------------------------------------------------------------------------

def join_order(tables, joins):
    """Left-deep order: first FROM table, then always the first FROM table connected to the joined set."""
    order = [tables[0][1]]
    rest = [a for _, a in tables[1:]]
    while rest:
        nxt = next((a for a in rest if any({x[0], y[0]} == {a, b} for x, y in joins for b in order)), None)
        if nxt is None:
            raise UnsupportedQuery(f"join graph is not connected: {order} vs {rest}")
        order.append(nxt)
        rest.remove(nxt)
    return order


_AGG = re.compile(r"\b(MIN|MAX|SUM|AVG|COUNT)\s*\(", re.I)


def build_plan(q, filters, joins, source):
    """Input plan of a parsed query. `filters`: alias -> [conjunct]; `joins`: [((a, col), (b, col))]."""
    tables = q["tables"]
    table_of = {a: t for t, a in tables}
    nodes = []

    def add(pact, contents, preds=(), node_type="pact", driver=None, ship="Forward"):
        node = {"id": len(nodes), "type": node_type, "pact": pact, "contents": contents, "parallelism": "1"}
        if preds:
            node["predecessors"] = [{"id": p, "ship_strategy": ship, "exchange_mode": "PIPELINED"} for p in preds]
        if driver:
            node["driver_strategy"] = driver
        nodes.append(node)
        return node["id"]

    def scan(alias):
        name = table_of[alias] if alias == table_of[alias] else f"{table_of[alias]} AS {alias}"
        nid = add("Data Source", name, node_type="source")
        if filters.get(alias):
            nid = add("Filter", " AND ".join(filters[alias]), [nid], driver="FlatMap")
        return nid

    order = join_order(tables, joins)
    current = scan(order[0])
    joined = {order[0]}
    for a in order[1:]:
        right = scan(a)
        keys = [f"{x[0]}.{x[1]} = {y[0]}.{y[1]}" for x, y in joins
                if (x[0] == a and y[0] in joined) or (y[0] == a and x[0] in joined)]
        current = add("Join", "join on " + " AND ".join(keys), [current, right],
                      driver="Hybrid Hash (build: second input)", ship="Hash Partition")
        joined.add(a)
    select = q["select"]
    if q["group_by"] or q["distinct"]:
        current = add("GroupCombine", f"combine: {select}", [current], driver="Sorted Combine")
        current = add("GroupReduce", f"reduce: {select}", [current], driver="Sorted Group Reduce",
                      ship="Hash Partition")
    elif _AGG.search(select):
        current = add("Reduce", f"combine: {select}", [current], driver="Reduce")
        current = add("Reduce", f"reduce: {select}", [current], driver="Reduce", ship="Forward")
    if q["order_by"]:
        current = add("Sort-Partition", "order by", [current], driver="Sort-Partition")
    add("Data Sink", "org.apache.flink.api.java.io.DiscardingOutputFormat", [current], node_type="sink")
    return {"source": source, "query": q.get("name", ""), "netRunTime": -1, "executionPlan": {"nodes": nodes}}
