package instantiation.dataset.generic

import instantiation.platform.sql.SQLDialect

/**
  * A filter predicate on one column of a dataset table, e.g. `title.production_year > 2000`.
  *
  * Supported operators (SQL semantics: a NULL value never satisfies a predicate, except `is null`):
  *  - `=`, `!=`, `<`, `<=`, `>`, `>=` with one value (text columns compare lexicographically)
  *  - `between` with two values (inclusive)
  *  - `in` with one or more values
  *  - `like`, `not like` with one SQL pattern (`%` and `_`, no escape character); text columns only
  *  - `is null`, `is not null` without values
  *  - `mod` with two values `[m, r]`: `column % m == r`; integer columns only. Offered for tables the
  *    seed queries never filter (on a dense key, `id % m == r` keeps about 1/m of the rows).
  *
  * Text columns whose text does not order like their values (decimals, ...) only allow `=`, `!=`, `in`,
  * `like`, `not like` and the NULL tests.
  *
  * @param selectivity fraction of the table's rows that satisfy the predicate, if measured
  */
case class Predicate(table: TableColumns, field: String, op: String, values: Seq[String], selectivity: Option[Double]) {

  import Predicate._

  private val column: Column = table.column(field)

  require(Ops.contains(op), s"$this: unsupported operator '$op' (supported: ${Ops.mkString(", ")})")
  require(arityOk, s"$this: wrong number of values for '$op'")
  require(!(column.isInt && TextOnly.contains(op)), s"$this: '$op' needs a text column")
  require(!(!column.isInt && IntOnly.contains(op)), s"$this: '$op' needs an integer column")
  require(column.isInt || column.ordered || !Ordering.contains(op),
    s"$this: '$op' compares the text of ${table.name}.$field (${column.sqlType}), which does not order like its values")
  if (column.isInt) values.foreach(v => require(scala.util.Try(v.trim.toLong).isSuccess, s"$this: '$v' is not an integer"))

  private def arityOk: Boolean = op match {
    case "is null" | "is not null" => values.isEmpty
    case "between" | "mod" => values.size == 2
    case "in" => values.nonEmpty
    case _ => values.size == 1
  }

  /** Scala boolean expression over tuple variable `x`, e.g. `(x._5 != Long.MinValue && x._5 > 2000L)`. */
  def condition(parsersObject: String): String = {
    val v = s"x._${table.index(field)}"
    if (column.isInt) {
      val notNull = s"$v != $IntNull"
      def l(s: String) = s"${s.trim.toLong}L"
      op match {
        case "=" => s"($notNull && $v == ${l(values.head)})"
        case "!=" | "<" | "<=" | ">" | ">=" => s"($notNull && $v $op ${l(values.head)})"
        case "between" => s"($notNull && $v >= ${l(values(0))} && $v <= ${l(values(1))})"
        case "in" => s"($v match { case ${values.map(l).mkString(" | ")} => true; case _ => false })"
        case "mod" => s"($notNull && $v % ${l(values(0))} == ${l(values(1))})"
        case "is null" => s"($v == $IntNull)"
        case "is not null" => s"($notNull)"
      }
    } else {
      val notNull = s"$v != null"
      op match {
        case "=" => s"($notNull && $v == ${lit(values.head)})"
        case "!=" => s"($notNull && $v != ${lit(values.head)})"
        case "<" | "<=" | ">" | ">=" => s"($notNull && $v.compareTo(${lit(values.head)}) $op 0)"
        case "between" => s"($notNull && $v.compareTo(${lit(values(0))}) >= 0 && $v.compareTo(${lit(values(1))}) <= 0)"
        case "in" => s"($v match { case ${values.map(lit).mkString(" | ")} => true; case _ => false })"
        case "like" => s"($notNull && $parsersObject.like($v, ${lit(values.head)}))"
        case "not like" => s"($notNull && !$parsersObject.like($v, ${lit(values.head)}))"
        case "is null" => s"($v == null)"
        case "is not null" => s"($notNull)"
      }
    }
  }

  /** Human-readable form, e.g. `title.production_year > 2000` (used in the job info JSON). */
  def sql: String = op match {
    case "is null" | "is not null" => s"${table.name}.$field $op"
    case "between" => s"${table.name}.$field between ${q(values(0))} and ${q(values(1))}"
    case "in" => s"${table.name}.$field in (${values.map(q).mkString(", ")})"
    case "mod" => s"${table.name}.$field % ${values(0)} = ${values(1)}"
    case _ => s"${table.name}.$field $op ${q(values.head)}"
  }

  /**
    * SQL condition on the bare column, e.g. `production_year > 2000`, in the given dialect (only LIKE
    * patterns differ). SQL's NULL semantics match the generated dataflow code (NULL never satisfies a
    * predicate except `is null`).
    */
  def sqlCondition(dialect: SQLDialect): String = op match {
    case "is null" => s"$field IS NULL"
    case "is not null" => s"$field IS NOT NULL"
    case "between" => s"$field BETWEEN ${q(values(0))} AND ${q(values(1))}"
    case "in" => s"$field IN (${values.map(q).mkString(", ")})"
    case "mod" => s"$field % ${values(0)} = ${values(1)}"
    case "like" => s"$field LIKE ${q(dialect.likePattern(values.head))}"
    case "not like" => s"$field NOT LIKE ${q(dialect.likePattern(values.head))}"
    case "!=" => s"$field <> ${q(values.head)}"
    case _ => s"$field $op ${q(values.head)}"
  }

  private def q(s: String) = if (column.isInt) s.trim else "'" + s.replace("'", "''") + "'"

  override def toString: String = s"Predicate(${table.name}.$field $op ${values.mkString("[", ", ", "]")})"
}

object Predicate {
  val Ops: Seq[String] = Seq("=", "!=", "<", "<=", ">", ">=", "between", "in", "like", "not like", "is null", "is not null", "mod")
  val TextOnly: Set[String] = Set("like", "not like")
  val IntOnly: Set[String] = Set("mod")
  val Ordering: Set[String] = Set("<", "<=", ">", ">=", "between")

  /** How NULL integers are represented in the generated tuples. */
  val IntNull = "Long.MinValue"

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
