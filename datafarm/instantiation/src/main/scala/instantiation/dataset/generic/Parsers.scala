package instantiation.dataset.generic

import instantiation.dataset.RecordOrder

/**
  * Parse functions for the data files of a dataset, generated into each Flink, Wayang and Spark job.
  * They follow the dataset's [[DataFormat]] with the rules of datafarm/datasets/csvformat.py: fields
  * separated by the delimiter; a field starting with the quote character runs to the closing quote, inside
  * which the escape character takes the next character literally and a doubled quote is one quote; an
  * unquoted field equal to the NULL marker is NULL; missing trailing fields are NULL; an integer that does
  * not parse is NULL. Integers are `Long` (NULL -> `Long.MinValue`), other columns `String` (NULL -> null).
  */
object Parsers {

  def code(objectName: String, format: DataFormat, tables: Seq[Table]): String = {
    import Predicate.{charLit, lit}
    val parsers = tables.groupBy(_.name).values.map(_.head).toSeq.sortBy(_.name).map { t =>
      val values = t.columns.zipWithIndex.map { case (c, i) => if (c.isInt) s"int(f($i))" else s"f($i)" }
      s"  def ${t.name}(line: String): ${t.tupleType} = {\n" +
        s"    val f = split(line, ${t.columns.size})\n" +
        s"    ${values.mkString("(", ", ", ")")}\n" +
        "  }"
    }
    val quote = format.quote.map(charLit).getOrElse("'\\u0000'")
    val escape = format.escape.map(charLit).getOrElse("'\\u0000'")

    (Seq(
      s"/** Parsing and predicate helpers for [[$objectName]]. */",
      s"object $objectName {",
      "",
      "  /** NULL integers are represented as Long.MinValue, NULL text as null. */",
      "  val NullInt: Long = Long.MinValue",
      s"  val Delimiter: Char = ${charLit(format.delimiter)}",
      s"  val HasQuote: Boolean = ${format.quote.isDefined}",
      s"  val Quote: Char = $quote",
      s"  val HasEscape: Boolean = ${format.escape.isDefined}",
      s"  val Escape: Char = $escape",
      s"  val NullValue: String = ${lit(format.nullValue)}",
      "",
      "  def int(s: String): Long =",
      "    if (s == null || s.isEmpty) NullInt",
      "    else try s.trim.toLong catch { case _: NumberFormatException => NullInt }",
      "",
      "  private def field(s: String): String = if (s == NullValue) null else s",
      "",
      "  /** Splits a line into exactly n fields. */",
      "  def split(line: String, n: Int): Array[String] = {",
      "    val out = new Array[String](n)",
      "    val len = if (line.endsWith(\"\\r\")) line.length - 1 else line.length",
      "    val sb = new java.lang.StringBuilder",
      "    var i = 0",
      "    var col = 0",
      "    while (col < n && i <= len) {",
      "      if (HasQuote && i < len && line.charAt(i) == Quote) {",
      "        sb.setLength(0)",
      "        i += 1",
      "        var open = true",
      "        while (open && i < len) {",
      "          val c = line.charAt(i)",
      "          if (HasEscape && c == Escape && i + 1 < len) { sb.append(line.charAt(i + 1)); i += 2 }",
      "          else if (c == Quote && i + 1 < len && line.charAt(i + 1) == Quote) { sb.append(Quote); i += 2 }",
      "          else if (c == Quote) { open = false; i += 1 }",
      "          else { sb.append(c); i += 1 }",
      "        }",
      "        while (i < len && line.charAt(i) != Delimiter) i += 1",
      "        out(col) = sb.toString",
      "      } else {",
      "        val j = line.indexOf(Delimiter, i)",
      "        val end = if (j < 0 || j > len) len else j",
      "        out(col) = field(line.substring(i, end))",
      "        i = end",
      "      }",
      "      col += 1",
      "      i += 1",
      "    }",
      "    out",
      "  }",
      "",
      "  /** SQL LIKE: '%' matches any sequence, '_' any single character. */",
      "  def like(s: String, p: String): Boolean = {",
      "    var si = 0",
      "    var pi = 0",
      "    var starP = -1",
      "    var starS = 0",
      "    while (si < s.length) {",
      "      if (pi < p.length && p.charAt(pi) == '%') { starP = pi; starS = si; pi += 1 }",
      "      else if (pi < p.length && (p.charAt(pi) == '_' || p.charAt(pi) == s.charAt(si))) { si += 1; pi += 1 }",
      "      else if (starP >= 0) { pi = starP + 1; starS += 1; si = starS }",
      "      else return false",
      "    }",
      "    while (pi < p.length && p.charAt(pi) == '%') pi += 1",
      "    pi == p.length",
      "  }",
      "",
      RecordOrder.code,
      ""
    ) ++ parsers :+ "}").mkString("\n")
  }
}
