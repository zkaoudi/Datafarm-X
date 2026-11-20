package instantiation.dataset.generic

import instantiation.platform.sql.SQLDialect

/**
  * A filter predicate on one column of a dataset table, e.g. `title.production_year > 2000`.
  *
  * Supported operators (SQL semantics: a NULL value never satisfies a predicate, except `is null`):
  *  - `=`, `!=`, `<`, `<=`, `>`, `>=` with one value (text compares lexicographically, by UTF-16 code unit)
  *  - `between` with two values (inclusive)
  *  - `in` with one or more values
  *  - `like`, `not like` with one SQL pattern (`%` and `_`, no escape character); text columns only
  *  - `is null`, `is not null` without values
  *  - `mod` with two values `[m, r]`: `column % m == r`; integer columns only. Offered for tables the
  *    seed queries never filter (on a dense key, `id % m == r` keeps about 1/m of the rows).
  *
  * Values are given as text: integers, decimal numbers, ISO dates (yyyy-MM-dd) and timestamps
  * (yyyy-MM-dd HH:mm[:ss[.fff]]), or text. Text columns whose text does not order like their values
  * (BOOLEAN, UUID, ...) only allow `=`, `!=`, `in`, `like`, `not like` and the NULL tests.
  *
  * @param selectivity fraction of the table's rows that satisfy the predicate, if measured
  */
case class Predicate(table: TableColumns, field: String, op: String, values: Seq[String], selectivity: Option[Double]) {

  import Predicate._

  private val column: Column = table.column(field)

  require(Ops.contains(op), s"$this: unsupported operator '$op' (supported: ${Ops.mkString(", ")})")
  require(arityOk, s"$this: wrong number of values for '$op'")
  require(column.isText || !TextOnly.contains(op), s"$this: '$op' needs a text column")
  require(column.isInt || !IntOnly.contains(op), s"$this: '$op' needs an integer column")
  require(!column.isText || column.ordered || !Ordering.contains(op),
    s"$this: '$op' compares the text of ${table.name}.$field (${column.sqlType}), which does not order like its values")

  /** The values as the jobs compare them: Long for int, date and timestamp columns, Double for decimals. */
  private val parsed: Seq[Any] =
    if (op == "mod") values.map(v => Values.int(v).getOrElse(throw new IllegalArgumentException(s"$this: '$v' is not an integer")))
    else values.map(v => Values.parse(column.kind, v).getOrElse(
      throw new IllegalArgumentException(s"$this: '$v' is not a valid ${column.kind}")))

  private def arityOk: Boolean = op match {
    case "is null" | "is not null" => values.isEmpty
    case "between" | "mod" => values.size == 2
    case "in" => values.nonEmpty
    case _ => values.size == 1
  }

  /** Scala literal of a parsed value. */
  private def scala(v: Any): String = v match {
    case l: Long => s"${l}L"
    case d: Double => java.lang.Double.toString(d)
    case t: String => lit(t)
  }

  /** Scala boolean expression over tuple variable `x`, e.g. `(x._5 != Long.MinValue && x._5 > 2000L)`. */
  def condition(parsersObject: String): String = {
    val v = s"x._${table.index(field)}"
    val isNull = column.nullTest(v)
    val notNull = s"!($isNull)"
    val l = parsed.map(scala)
    op match {
      case "is null" => s"($isNull)"
      case "is not null" => s"($notNull)"
      case "in" => s"($notNull && (${l.map(x => s"$v == $x").mkString(" || ")}))"
      case "like" => s"($notNull && $parsersObject.like($v, ${l.head}))"
      case "not like" => s"($notNull && !$parsersObject.like($v, ${l.head}))"
      case "mod" => s"($notNull && $v % ${l(0)} == ${l(1)})"
      case _ if column.isText => op match {
        case "=" => s"($notNull && $v == ${l.head})"
        case "!=" => s"($notNull && $v != ${l.head})"
        case "between" => s"($notNull && $v.compareTo(${l(0)}) >= 0 && $v.compareTo(${l(1)}) <= 0)"
        case _ => s"($notNull && $v.compareTo(${l.head}) $op 0)"
      }
      case "between" => s"($notNull && $v >= ${l(0)} && $v <= ${l(1)})"
      case _ => s"($notNull && $v $op ${l.head})"
    }
  }

  /** Human-readable form, e.g. `title.production_year > 2000` (used in the job info JSON). */
  def sql: String = condition(SQLDialect.Standard, s"${table.name}.$field")

  /**
    * SQL condition on the bare column, e.g. `production_year > 2000`, in the given dialect. SQL's NULL
    * semantics match the generated dataflow code (NULL never satisfies a predicate except `is null`).
    */
  def sqlCondition(dialect: SQLDialect): String = condition(dialect, field)

  private def condition(dialect: SQLDialect, col: String): String = {
    def q(v: Any): String = (column.kind, v) match {
      case ("date", d: Long) => dialect.dateLiteral(Values.formatDate(d))
      case ("timestamp", t: Long) => dialect.timestampLiteral(Values.formatTimestamp(t))
      case ("text", t: String) => "'" + t.replace("'", "''") + "'"
      case (_, d: Double) => java.math.BigDecimal.valueOf(d).toPlainString
      case (_, other) => other.toString
    }
    op match {
      case "is null" => s"$col IS NULL"
      case "is not null" => s"$col IS NOT NULL"
      case "between" => s"$col BETWEEN ${q(parsed(0))} AND ${q(parsed(1))}"
      case "in" => s"$col IN (${parsed.map(q).mkString(", ")})"
      case "mod" => s"$col % ${parsed(0)} = ${parsed(1)}"
      case "like" => s"$col LIKE ${q(dialect.likePattern(values.head))}"
      case "not like" => s"$col NOT LIKE ${q(dialect.likePattern(values.head))}"
      case "!=" => s"$col <> ${q(parsed.head)}"
      case _ => s"$col $op ${q(parsed.head)}"
    }
  }

  override def toString: String = s"Predicate(${table.name}.$field $op ${values.mkString("[", ", ", "]")})"
}

/**
  * Values of the column kinds, read as datafarm/datasets/csvformat.py and the generated parsers read them.
  * The generated jobs get the same functions as Scala source ([[Values.code]]).
  */
object Values {
  private val IntRe = """[+-]?[0-9]+""".r
  private val DecimalRe = """[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?""".r
  private val DateRe = """([0-9]{4})-([0-9]{2})-([0-9]{2})""".r
  private val TimestampRe = """([0-9]{4})-([0-9]{2})-([0-9]{2})[ T]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]{1,9}))?)?""".r

  private def trim(s: String) = s.trim

  def int(s: String): Option[Long] = trim(s) match {
    case t @ IntRe() => scala.util.Try(t.toLong).toOption
    case _ => None
  }

  def decimal(s: String): Option[Double] = trim(s) match {
    case t @ DecimalRe() => Some(t.toDouble)
    case _ => None
  }

  def date(s: String): Option[Long] = trim(s) match {
    case DateRe(y, m, d) => scala.util.Try(java.time.LocalDate.of(y.toInt, m.toInt, d.toInt).toEpochDay).toOption.filter(_ => y.toInt >= 1)
    case _ => None
  }

  def timestamp(s: String): Option[Long] = trim(s) match {
    case TimestampRe(y, mo, d, h, mi, sec, frac) =>
      date(s"$y-$mo-$d").filter(_ => h.toInt <= 23 && mi.toInt <= 59 && Option(sec).forall(_.toInt <= 59)).map { days =>
        val ms = Option(frac).map(f => (f + "000").take(3).toLong).getOrElse(0L)
        ((days * 24 + h.toLong) * 60 + mi.toLong) * 60000L + Option(sec).map(_.toLong).getOrElse(0L) * 1000L + ms
      }
    case _ => None
  }

  def parse(kind: String, s: String): Option[Any] = kind match {
    case "int" => int(s)
    case "decimal" => decimal(s)
    case "date" => date(s)
    case "timestamp" => timestamp(s)
    case _ => Some(s)
  }

  def formatDate(days: Long): String = java.time.LocalDate.ofEpochDay(days).toString

  def formatTimestamp(ms: Long): String = {
    val days = Math.floorDiv(ms, 86400000L)
    val rest = Math.floorMod(ms, 86400000L)
    f"${formatDate(days)} ${rest / 3600000}%02d:${rest / 60000 % 60}%02d:${rest / 1000 % 60}%02d.${rest % 1000}%03d"
  }

  /** Scala source of the parse functions of the generated jobs (members of their parser object). */
  val code: String =
    """  private val IntRe = "[+-]?[0-9]+".r
      |  private val DecimalRe = "[+-]?(?:[0-9]+\\.?[0-9]*|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?".r
      |  private val DateRe = "([0-9]{4})-([0-9]{2})-([0-9]{2})".r
      |  private val TimestampRe = "([0-9]{4})-([0-9]{2})-([0-9]{2})[ T]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\\.([0-9]{1,9}))?)?".r
      |
      |  /** An integer, or Long.MinValue for NULL and text that is not an integer. */
      |  def int(s: String): Long =
      |    if (s == null) Long.MinValue
      |    else s.trim match {
      |      case t @ IntRe() => try t.toLong catch { case _: NumberFormatException => Long.MinValue }
      |      case _ => Long.MinValue
      |    }
      |
      |  /** A decimal number, or NaN for NULL and text that is not a decimal number. */
      |  def dec(s: String): Double =
      |    if (s == null) Double.NaN
      |    else s.trim match {
      |      case t @ DecimalRe() => t.toDouble
      |      case _ => Double.NaN
      |    }
      |
      |  /** Days since 1970-01-01 of an ISO date, or Long.MinValue for NULL and invalid dates. */
      |  def date(s: String): Long =
      |    if (s == null) Long.MinValue
      |    else s.trim match {
      |      case DateRe(y, m, d) if y.toInt >= 1 =>
      |        try java.time.LocalDate.of(y.toInt, m.toInt, d.toInt).toEpochDay
      |        catch { case _: java.time.DateTimeException => Long.MinValue }
      |      case _ => Long.MinValue
      |    }
      |
      |  /** Milliseconds since 1970-01-01 00:00 of an ISO timestamp without time zone, or Long.MinValue. */
      |  def timestamp(s: String): Long =
      |    if (s == null) Long.MinValue
      |    else s.trim match {
      |      case TimestampRe(y, mo, d, h, mi, sec, frac) if h.toInt <= 23 && mi.toInt <= 59 && (sec == null || sec.toInt <= 59) =>
      |        val days = date(y + "-" + mo + "-" + d)
      |        if (days == Long.MinValue) Long.MinValue
      |        else {
      |          val ms = if (frac == null) 0L else (frac + "000").take(3).toLong
      |          ((days * 24 + h.toLong) * 60 + mi.toLong) * 60000L + (if (sec == null) 0L else sec.toLong * 1000L) + ms
      |        }
      |      case _ => Long.MinValue
      |    }""".stripMargin
}

object Predicate {
  val Ops: Seq[String] = Seq("=", "!=", "<", "<=", ">", ">=", "between", "in", "like", "not like", "is null", "is not null", "mod")
  val TextOnly: Set[String] = Set("like", "not like")
  val IntOnly: Set[String] = Set("mod")
  val Ordering: Set[String] = Set("<", "<=", ">", ">=", "between")


  /** Scala string literal for `s`. */
  def lit(s: String): String = {
    val sb = new StringBuilder("\"")
    s.foreach {
      case '\\' => sb.append("\\\\")
      case '"' => sb.append("\\\"")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case c if c < ' ' => sb.append(f"\\u${c.toInt}%04x")
      case c => sb.append(c)
    }
    sb.append('"').toString
  }

  /** Scala character literal for `c`. */
  def charLit(c: Char): String = c match {
    case '\\' => "'\\\\'"
    case '\'' => "'\\''"
    case '\n' => "'\\n'"
    case '\r' => "'\\r'"
    case '\t' => "'\\t'"
    case c if c < ' ' => f"'\\u${c.toInt}%04x'"
    case c => s"'$c'"
  }
}
