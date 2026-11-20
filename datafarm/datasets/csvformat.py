"""
How the data files of a dataset are read: the same rules in Python (statistics, SQL loading) and in the
parsers DataFarm generates into every Flink, Wayang and Spark job, so all platforms see the same rows.

A row is one line, split into exactly as many fields as the table has columns:
  - fields are separated by `delimiter`; a field that starts with `quote` runs to the closing quote,
    inside which `escape` takes the next character literally and a doubled quote is one quote;
  - an unquoted field equal to `null` is NULL (by default the empty field), a quoted one never is;
  - missing trailing fields are NULL and extra trailing fields are ignored (so TPC-H's trailing '|' is fine);
  - an integer column whose text is not an integer is NULL;
  - with `header`, the first line of each file is skipped.

Integers are 64-bit; NULL integers are represented as Long.MinValue (NULL_INT) in the generated jobs.
"""
import re

NULL_INT = -(2 ** 63)  # Long.MinValue
_INT_RE = re.compile(r"[+-]?[0-9]+\Z")

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

    def rows(self, path, columns):
        """Yields the rows of a data file as lists: int for integer columns, str for text, None for NULL.
        `columns` are (name, is_int) pairs in file order."""
        n = len(columns)
        is_int = [c[1] for c in columns]
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
                yield [(None if (v := parse_int(raw[k])) == NULL_INT else v) if is_int[k] else raw[k]
                       for k in range(n)]

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
