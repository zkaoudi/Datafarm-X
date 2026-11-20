"""
Read the CREATE TABLE statements of a dataset's schema.sql.

Understood: column names and types, NOT NULL, PRIMARY KEY (on a column or as a table constraint),
REFERENCES (on a column) and FOREIGN KEY ... REFERENCES ... (as a table constraint). Other statements
(CREATE INDEX, ALTER TABLE, ...) and other constraints (UNIQUE, CHECK, DEFAULT) are ignored.

Column types are mapped to the two kinds the generated jobs know:
  - "int": the integer types (SMALLINT, INT, INTEGER, BIGINT, TINYINT, SERIAL, ...), 64-bit in the jobs;
  - "text": everything else. CHAR/VARCHAR/TEXT columns are text anyway; other types (DATE, TIMESTAMP,
    DECIMAL, ...) are carried as their text in this version, and only compared with =, !=, IN, LIKE and
    NULL tests, except dates and timestamps, whose ISO text also orders correctly with <, >, BETWEEN.
"""
import re

INT_TYPES = {"int", "integer", "bigint", "smallint", "tinyint", "mediumint", "int2", "int4", "int8",
             "serial", "bigserial", "smallserial", "hugeint", "long"}
TEXT_TYPES = {"text", "varchar", "char", "character", "nvarchar", "nchar", "string", "clob",
              "character varying", "varying", "bpchar"}
ORDERED_TEXT_TYPES = {"date", "timestamp", "datetime", "datetime2", "timestamptz", "time"}


def _strip_comments(sql):
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return "\n".join(re.sub(r"--.*$", "", l) for l in sql.splitlines())


def _split_top(s, sep=","):
    """Split on `sep` outside parentheses and quotes."""
    parts, buf, depth, q = [], [], 0, None
    for c in s:
        if q:
            buf.append(c)
            if c == q:
                q = None
            continue
        if c in "'\"`":
            q = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        if c == sep and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(c)
    if "".join(buf).strip():
        parts.append("".join(buf).strip())
    return parts


def _ident(s):
    s = s.strip()
    if len(s) >= 2 and s[0] in "\"`[" and s[-1] in "\"`]":
        return s[1:-1]
    return s.lower()


def _names(s):
    return [_ident(x) for x in s.split(",") if x.strip()]


def type_kind(sql_type):
    """'int' or 'text' for a declared SQL type, and whether its text orders like its values."""
    base = re.sub(r"\(.*", "", sql_type.strip().lower()).strip()
    base = re.sub(r"\s+(unsigned|signed)$", "", base)
    base = re.sub(r"\s+with(out)? time zone$", "", base)
    first = base.split()[0] if base else ""
    if base in INT_TYPES or first in INT_TYPES:
        return "int", True
    if base in TEXT_TYPES or first in TEXT_TYPES:
        return "text", True
    if base in ORDERED_TEXT_TYPES or first in ORDERED_TEXT_TYPES:
        return "text", True
    return "text", False


_COL_RE = re.compile(
    r"^(?P<name>\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|\w+)\s+(?P<type>.*?)"
    r"(?=\s+(?:NOT\s+NULL|NULL|PRIMARY\s+KEY|REFERENCES|UNIQUE|DEFAULT|CHECK|CONSTRAINT|COLLATE|GENERATED)\b|$)",
    re.I | re.S)


def parse_schema(sql):
    """-> list of tables: {name, columns: [{name, sqlType, type, ordered, nullable}], primaryKey: [..],
    foreignKeys: [{columns, table, refColumns}]}, in file order."""
    tables = []
    sql = _strip_comments(sql)
    for m in re.finditer(r"CREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.\"`\[\]]+)\s*\(",
                         sql, re.I):
        name = _ident(m.group(1).split(".")[-1])
        # the body runs to the matching parenthesis
        depth, i = 1, m.end()
        while i < len(sql) and depth:
            depth += {"(": 1, ")": -1}.get(sql[i], 0)
            i += 1
        body = sql[m.end():i - 1]
        columns, pk, fks = [], [], []
        for item in _split_top(body):
            flat = re.sub(r"\s+", " ", item).strip()
            up = flat.upper()
            if up.startswith("CONSTRAINT "):
                flat = re.sub(r"^CONSTRAINT\s+\S+\s+", "", flat, flags=re.I)
                up = flat.upper()
            if up.startswith("PRIMARY KEY"):
                pk = _names(re.search(r"\((.*?)\)", flat).group(1))
                continue
            if up.startswith("FOREIGN KEY"):
                fm = re.match(r"FOREIGN KEY\s*\((.*?)\)\s*REFERENCES\s+([\w.\"`\[\]]+)\s*(?:\((.*?)\))?", flat, re.I)
                if fm:
                    fks.append({"columns": _names(fm.group(1)), "table": _ident(fm.group(2).split(".")[-1]),
                                "refColumns": _names(fm.group(3)) if fm.group(3) else None})
                continue
            if re.match(r"(UNIQUE|CHECK|INDEX|KEY)\b", up):
                continue
            cm = _COL_RE.match(flat)
            if not cm:
                raise ValueError(f"table {name}: can not read the column definition '{flat}'")
            col = _ident(cm.group("name"))
            sql_type = cm.group("type").strip()
            rest = flat[cm.end():].upper()
            kind, ordered = type_kind(sql_type)
            is_pk = "PRIMARY KEY" in rest
            if is_pk:
                pk = [col]
            columns.append({"name": col, "sqlType": sql_type, "type": kind, "ordered": ordered,
                            "nullable": not ("NOT NULL" in rest or is_pk)})
            rm = re.search(r"REFERENCES\s+([\w.\"`\[\]]+)\s*(?:\((.*?)\))?", flat[cm.end():], re.I)
            if rm:
                fks.append({"columns": [col], "table": _ident(rm.group(1).split(".")[-1]),
                            "refColumns": _names(rm.group(2)) if rm.group(2) else None})
        if not columns:
            raise ValueError(f"table {name} has no columns")
        for c in columns:
            if c["name"] in pk:
                c["nullable"] = False
        tables.append({"name": name, "columns": columns, "primaryKey": pk, "foreignKeys": fks})
    if not tables:
        raise ValueError("no CREATE TABLE statement found")
    names = [t["name"] for t in tables]
    dup = {n for n in names if names.count(n) > 1}
    if dup:
        raise ValueError(f"tables defined twice: {', '.join(sorted(dup))}")
    # a FOREIGN KEY without column list references the primary key
    by_name = {t["name"]: t for t in tables}
    for t in tables:
        for fk in t["foreignKeys"]:
            if fk["refColumns"] is None:
                ref = by_name.get(fk["table"])
                fk["refColumns"] = ref["primaryKey"] if ref else []
    return tables
