"""
How the data files of a dataset are read: the same rules in Python (statistics, SQL loading) and in the
parsers DataFarm generates into every Flink, Wayang and Spark job, so all platforms see the same rows.

A row is one line, split into exactly as many fields as the table has columns:
  - fields are separated by `delimiter`; a field that starts with `quote` runs to the closing quote,
    inside which `escape` takes the next character literally and a doubled quote is one quote;
  - an unquoted field equal to `null` is NULL (by default the empty field), a quoted one never is;
  - missing trailing fields are NULL and extra trailing fields are ignored (so TPC-H's trailing '|' is fine);
  - a value that does not parse for its column's kind is NULL;
  - with `header`, the first line of each file is skipped.

Values by column kind (see schema.py), and how the generated jobs hold them:
  - int: an optionally signed integer, 64-bit (Long; NULL is Long.MinValue, NULL_INT);
  - decimal: a decimal number such as -12.50, 3, .5 or 1e-3, as a 64-bit float (Double; NULL is NaN);
  - date: ISO yyyy-MM-dd, as days since 1970-01-01 (Long; NULL is Long.MinValue);
  - timestamp: ISO yyyy-MM-dd HH:mm[:ss[.fraction]] (or with 'T'), without time zone, as milliseconds since
    1970-01-01 00:00 (Long; NULL is Long.MinValue); digits beyond milliseconds are dropped;
  - text: the field as it is (String; NULL is null).
All values are trimmed of leading and trailing whitespace before parsing, except text.
"""
import datetime
import math
import re

NULL_INT = -(2 ** 63)  # Long.MinValue
_INT_RE = re.compile(r"[+-]?[0-9]+\Z")
_DECIMAL_RE = re.compile(r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\Z")
_DATE_RE = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})\Z")
_TIMESTAMP_RE = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})[ T]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]{1,9}))?)?\Z")
_EPOCH = datetime.date(1970, 1, 1)
KINDS = ("int", "decimal", "date", "timestamp", "text")

DEFAULTS = {
    "extension": ".csv",
    "delimiter": ",",
    "quote": '"',
    "escape": None,
    "null": "",
    "header": False,
    "encoding": "utf-8",
}


class Format:
    def __init__(self, extension=".csv", delimiter=",", quote='"', escape=None, null="", header=False,
                 encoding="utf-8"):
        if not isinstance(delimiter, str) or len(delimiter) != 1:
            raise ValueError(f"format.delimiter must be one character, not {delimiter!r}")
        for name, v in (("quote", quote), ("escape", escape)):
            if v is not None and (not isinstance(v, str) or len(v) != 1):
                raise ValueError(f"format.{name} must be one character or null, not {v!r}")
        if quote is not None and quote == delimiter:
            raise ValueError("format.quote and format.delimiter must differ")
        self.extension = extension
        self.delimiter = delimiter
        self.quote = quote
        self.escape = escape
        self.null = null
        self.header = bool(header)
        self.encoding = encoding

    @classmethod
    def from_json(cls, d):
        d = dict(DEFAULTS, **(d or {}))
        unknown = set(d) - set(DEFAULTS)
        if unknown:
            raise ValueError(f"unknown format settings: {', '.join(sorted(unknown))} (known: {', '.join(DEFAULTS)})")
        return cls(**d)

    def to_json(self):
        return {k: getattr(self, k) for k in DEFAULTS}

    def split(self, line, n):
        """One line (without its line break) -> exactly n fields; None = NULL."""
        d, q, e = self.delimiter, self.quote, self.escape
        if line.endswith("\r"):
            line = line[:-1]
        if q is None or q not in line:
            parts = line.split(d, n)[:n]
            out = [None if p == self.null else p for p in parts]
            return out + [None] * (n - len(out))
        out = [None] * n
        length = len(line)
        i = col = 0
        while col < n and i <= length:
            if i < length and line[i] == q:
                buf = []
                i += 1
                open_ = True
                while open_ and i < length:
                    c = line[i]
                    if e is not None and c == e and i + 1 < length:
                        buf.append(line[i + 1]); i += 2
                    elif c == q and i + 1 < length and line[i + 1] == q:
                        buf.append(q); i += 2
                    elif c == q:
                        open_ = False; i += 1
                    else:
                        buf.append(c); i += 1
                while i < length and line[i] != d:
                    i += 1
                out[col] = "".join(buf)
            else:
                j = line.find(d, i)
                end = length if j < 0 else j
                v = line[i:end]
                out[col] = None if v == self.null else v
                i = end
            col += 1
            i += 1
        return out

    def rows(self, path, kinds):
        """Yields the rows of a data file for loading into a database, as lists: int for int columns, the text
        of the value for decimal columns, ISO text for dates (yyyy-MM-dd) and timestamps (yyyy-MM-dd
        HH:mm:ss.fff), the field for text, and None for NULL (and for values that do not parse, as in the jobs).
        `kinds`: the kind of each column, in file order."""
        n = len(kinds)
        conv = [sql_value(k) for k in kinds]
        # universal newlines: \n, \r and \r\n end a line, as for Java's BufferedReader in the generated jobs
        with open(path, encoding=self.encoding, errors="replace", newline=None) as fh:
            first = True
            for line in fh:
                if line.endswith("\n"):
                    line = line[:-1]
                if first and self.header:
                    first = False
                    continue
                first = False
                raw = self.split(line, n)
                yield [conv[k](raw[k]) for k in range(n)]

    def header_line(self, path):
        """The first line of a data file (what the generated jobs skip when `header` is set)."""
        with open(path, encoding=self.encoding, errors="replace", newline=None) as fh:
            line = fh.readline()
        return line[:-1] if line.endswith("\n") else line


def java_trim(s):
    """String.trim: removes chars <= ' ' at both ends."""
    i, j = 0, len(s)
    while i < j and s[i] <= " ":
        i += 1
    while j > i and s[j - 1] <= " ":
        j -= 1
    return s[i:j]


def parse_int(s):
    """The generated parsers' int(): NULL_INT for NULL, empty or non-integer text and values beyond 64 bits."""
    if s is None or s == "":
        return NULL_INT
    t = java_trim(s)
    if not _INT_RE.match(t):
        return NULL_INT
    v = int(t)
    return v if -(2 ** 63) <= v < 2 ** 63 else NULL_INT


def parse_decimal(s):
    """The generated parsers' dec(): a float, or NaN for NULL and text that is not a decimal number."""
    if s is None:
        return math.nan
    t = java_trim(s)
    return float(t) if _DECIMAL_RE.match(t) else math.nan


def parse_date(s):
    """The generated parsers' date(): days since 1970-01-01, or NULL_INT for NULL and invalid dates."""
    if s is None:
        return NULL_INT
    m = _DATE_RE.match(java_trim(s))
    if not m:
        return NULL_INT
    try:
        return (datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3))) - _EPOCH).days
    except ValueError:
        return NULL_INT


def parse_timestamp(s):
    """The generated parsers' timestamp(): milliseconds since 1970-01-01 00:00, or NULL_INT."""
    if s is None:
        return NULL_INT
    m = _TIMESTAMP_RE.match(java_trim(s))
    if not m:
        return NULL_INT
    y, mo, d, h, mi = (int(m.group(i)) for i in range(1, 6))
    sec = int(m.group(6) or 0)
    frac = (m.group(7) or "") + "000"
    if h > 23 or mi > 59 or sec > 59:
        return NULL_INT
    try:
        days = (datetime.date(y, mo, d) - _EPOCH).days
    except ValueError:
        return NULL_INT
    return ((days * 24 + h) * 60 + mi) * 60_000 + sec * 1000 + int(frac[:3])


def parse(kind, s):
    """A field as the generated jobs hold it: int (NULL_INT for NULL), float (NaN for NULL) or str (None)."""
    if kind == "int":
        return parse_int(s)
    if kind == "decimal":
        return parse_decimal(s)
    if kind == "date":
        return parse_date(s)
    if kind == "timestamp":
        return parse_timestamp(s)
    return s


def is_null(kind, v):
    if kind == "decimal":
        return v != v  # NaN
    if kind == "text":
        return v is None
    return v == NULL_INT


def format_date(days):
    return (_EPOCH + datetime.timedelta(days=days)).isoformat()


def format_timestamp(ms):
    days, rest = divmod(ms, 86_400_000)
    h, rest = divmod(rest, 3_600_000)
    mi, rest = divmod(rest, 60_000)
    sec, milli = divmod(rest, 1000)
    return f"{format_date(days)} {h:02d}:{mi:02d}:{sec:02d}.{milli:03d}"


def sql_value(kind):
    """Field -> the value loaded into a database column of that kind (None for NULL), as the jobs read it."""
    if kind == "int":
        return lambda s: None if (v := parse_int(s)) == NULL_INT else v
    if kind == "decimal":
        return lambda s: None if parse_decimal(s) != parse_decimal(s) else java_trim(s)
    if kind == "date":
        return lambda s: None if (v := parse_date(s)) == NULL_INT else format_date(v)
    if kind == "timestamp":
        return lambda s: None if (v := parse_timestamp(s)) == NULL_INT else format_timestamp(v)
    return lambda s: s


def literal(kind, v):
    """A constant of a filter (from a query or dataset.json) as the jobs compare it; ValueError if invalid."""
    if kind == "int":
        x = parse_int(str(v))
    elif kind == "decimal":
        x = parse_decimal(str(v))
    elif kind == "date":
        x = parse_date(str(v))
    elif kind == "timestamp":
        x = parse_timestamp(str(v))
    else:
        return str(v)
    if is_null(kind, x):
        raise ValueError(f"'{v}' is not a valid {kind}")
    return x


def like(s, p):
    """SQL LIKE with '%' (any sequence) and '_' (any character), no escape character."""
    si = pi = 0
    star_p, star_s = -1, 0
    while si < len(s):
        if pi < len(p) and p[pi] == "%":
            star_p, star_s = pi, si
            pi += 1
        elif pi < len(p) and (p[pi] == "_" or p[pi] == s[si]):
            si += 1; pi += 1
        elif star_p >= 0:
            pi = star_p + 1; star_s += 1; si = star_s
        else:
            return False
    while pi < len(p) and p[pi] == "%":
        pi += 1
    return pi == len(p)


def java_mod(x, m):
    """Java/Scala % (truncates towards zero)."""
    r = abs(x) % abs(m)
    return -r if x < 0 else r


def utf16(s):
    """Sort key: Java compares strings by UTF-16 code units."""
    return s.encode("utf-16-be", "surrogatepass")
