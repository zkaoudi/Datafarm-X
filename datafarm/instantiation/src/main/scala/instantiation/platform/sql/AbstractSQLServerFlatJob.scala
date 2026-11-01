package instantiation.platform.sql

import instantiation.AbstractJob
import instantiation.JsonUtil
import instantiation.dataset.tpch.TPCHSql

import java.io.PrintWriter
import java.nio.file.Paths

import scala.collection.immutable.ListMap

/**
  * A generated job for `TARGET_PLATFORM = "SQLServerFlat"`: one flat SQL Server query, written as
  *
  *  - `<jobName>.txt`: the query text, e.g.
  *    `SELECT O_ORDERSTATUS, COUNT(*) FROM ORDERS, LINEITEM WHERE O_TOTALPRICE <= 144409.04 AND
  *     O_ORDERKEY = L_ORDERKEY GROUP BY O_ORDERSTATUS ORDER BY O_ORDERSTATUS DESC`
  *  - `<jobName>.json`: the same query as a dict with the keys FROM, SELECT, WHERE, WHERE_JOIN,
  *    GROUP BY, ORDER BY; RunGenerator.py turns it into `<jobName>.pickle`
  *
  * The queries are always valid SQL Server: tables do not repeat in FROM (the format has no aliases),
  * a GROUP BY query selects its group columns, and ORDER BY in a grouped query uses group columns
  * or aggregates.
  *
  * Tables are named `<prefix>TABLE` with the prefix from the environment variable
  * DATAFARM_SQLSERVER_TABLE_PREFIX (CONFIG.SQLSERVER_TABLE_PREFIX, e.g. "tpch.dbo."), unqualified by default.
  */
case class AbstractSQLServerFlatJob(jobBody: String, jobName: String) extends AbstractJob {

  import AbstractSQLServerFlatJob._

  // NB: only constructor parameters and defs here, AbstractJob calls fillBody() during construction
  override def fillBody(): String = assemble()._1

  override def fillSBT(): String = ""

  override def createProject(projectPath: String): Unit = {
    val (sql, dict) = assemble()
    println(s"Saving flat SQL Server job to ${Paths.get(projectPath, jobName + ".txt")}")
    write(Paths.get(projectPath, s"$jobName.txt").toString, sql + "\n")
    write(Paths.get(projectPath, s"$jobName.json").toString, JsonUtil.toJson(dict) + "\n")
  }

  private def write(path: String, content: String): Unit = {
    val w = new PrintWriter(new java.io.File(path), "UTF-8")
    try w.write(content) finally w.close()
  }

  /** (query text, dict) */
  def assemble(): (String, ListMap[String, Any]) = {
    val frags = jobBody.split("\n").map(_.trim).filter(_.startsWith(Marker))
      .map(l => JsonUtil.fromJson[Map[String, Any]](l.stripPrefix(Marker))).toSeq
    def ofOp(op: String) = frags.filter(_("op") == op)
    def str(f: Map[String, Any], k: String) = f(k).toString
    def int(f: Map[String, Any], k: String) = f(k).toString.toInt
    def strs(f: Map[String, Any], k: String) = f(k).asInstanceOf[Seq[Any]].map(_.toString)

    val tables = ofOp("FROM").map(str(_, "table"))
    if (tables.distinct.size != tables.size)
      throw new Exception(s"$jobName: a table occurs twice in FROM (${tables.mkString(", ")}); the flat format has no aliases")
    val prefix = tablePrefix

    // one filter per column (a second filter on the same column is dropped)
    val wheres = firstPer(ofOp("WHERE"))(str(_, "field"))
    val joins = ofOp("JOIN")
    val groups = ofOp("GROUP").map(str(_, "field")).distinct
    val hasGroup = groups.nonEmpty
    val hasOrder = ofOp("SORT").nonEmpty
    // without GROUP BY and ORDER BY, about half of the jobs aggregate (a job-level choice)
    val aggregate = hasGroup || (!hasOrder && Math.floorMod(jobName.hashCode, 2) == 1)

    def aggregateOf(op: String, numeric: Seq[String], seed: Int): (String, String) =
      if (op != "COUNT" && numeric.nonEmpty) (numeric(Math.floorMod(seed, numeric.size)), op) else ("*", "COUNT")

    val selects: Seq[(String, Option[String])] = ofOp("MAP").map { m =>
      val seed = int(m, "seed")
      if (!aggregate) {
        val cols = strs(m, "columns")
        (cols(Math.floorMod(seed + 7, cols.size)), None)
      } else {
        val (field, op) = aggregateOf(MapOperators(Math.floorMod(seed + 11, MapOperators.size)), strs(m, "numeric"), seed + 7)
        (field, Some(op))
      }
    }.distinct

    val orders: Seq[(String, String)] = ofOp("SORT").flatMap { s =>
      val s1 = int(s, "seed")
      val s2 = s1 / 2
      val order = if (Math.floorMod(s2, 2) == 0) "ASC" else "DESC"
      if (hasGroup) {
        if (Math.floorMod(s1, 2) == 0) {
          val (field, op) = aggregateOf(MapOperators(Math.floorMod(s1 + s2, MapOperators.size)), strs(s, "numeric"), s1 + s2)
          Seq((s"$op($field)", order))
        } else groups.map(g => (g, order))
      } else {
        val cols = strs(s, "groupColumns")
        Seq((cols(Math.floorMod(s1, cols.size)), order))
      }
    }
    val orderedOrders = firstPer(orders)(_._1) // one ORDER BY item per expression, first wins

    // ---- dict ----
    // no maps and no GROUP BY: all columns, listed explicitly (that parser reads SELECT * only for one unqualified table)
    val allColumns = if (selects.isEmpty && !hasGroup) ofOp("FROM").flatMap(strs(_, "columns")) else Seq()
    val selectEntries = allColumns.map(c => ListMap("FIELD" -> c)) ++ selects.map {
      case (f, Some(op)) => ListMap("FIELD" -> f, "OPERATOR" -> op)
      case (f, None) => ListMap("FIELD" -> f)
    } ++ (if (hasGroup) groups.filterNot(g => selects.contains((g, None))).map(g => ListMap("FIELD" -> g)) else Seq())
    val dict = ListMap[String, Any]("FROM" -> tables.map(t => prefix + t.toUpperCase)) ++
      (if (selectEntries.nonEmpty) ListMap("SELECT" -> selectEntries) else ListMap()) ++
      (if (wheres.nonEmpty) ListMap("WHERE" -> wheres.map(w => ListMap("FIELD" -> str(w, "field"), "OPERATOR" -> str(w, "operator"), "VALUE" -> str(w, "value")))) else ListMap()) ++
      (if (joins.nonEmpty) ListMap("WHERE_JOIN" -> joins.map(j => ListMap("FIELD" -> str(j, "field"), "LEFT" -> str(j, "left"), "RIGHT" -> str(j, "right")))) else ListMap()) ++
      (if (hasGroup) ListMap("GROUP BY" -> groups) else ListMap()) ++
      (if (orderedOrders.nonEmpty) ListMap("ORDER BY" -> orderedOrders.map { case (f, o) => ListMap("FIELD" -> f, "ORDER" -> o) }) else ListMap())

    // ---- text ----
    val selectItems = allColumns ++ (if (hasGroup) groups else Seq()) ++ selects.filterNot { case (f, op) => op.isEmpty && hasGroup && groups.contains(f) }.map {
      case (f, Some(op)) => s"$op($f)"
      case (f, None) => f
    }
    val conditions = wheres.map(w => s"${str(w, "field")} ${str(w, "operator")} ${str(w, "value")}") ++
      joins.map { j =>
        val suffix = str(j, "field").split("_", 2)(1)
        s"${prefixOf(str(j, "left"))}$suffix = ${prefixOf(str(j, "right"))}$suffix"
      }
    val sql = "SELECT " + selectItems.distinct.mkString(", ") +
      " FROM " + tables.map(t => prefix + t.toUpperCase).mkString(", ") +
      (if (conditions.nonEmpty) " WHERE " + conditions.mkString(" AND ") else "") +
      (if (hasGroup) " GROUP BY " + groups.mkString(", ") else "") +
      (if (orderedOrders.nonEmpty) " ORDER BY " + orderedOrders.map { case (f, o) => if (o == "DESC") s"$f DESC" else f }.mkString(", ") else "")
    (sql, dict)
  }
}

object AbstractSQLServerFlatJob {
  val Marker = "--#flat#--"

  val MapOperators: Seq[String] = Seq("COUNT", "AVG", "SUM")

  /** Columns aggregated with AVG/SUM. */
  val numericColumns: Map[String, Seq[String]] = Map(
    "lineitem" -> Seq("L_QUANTITY", "L_DISCOUNT"),
    "orders" -> Seq("O_TOTALPRICE"),
    "customer" -> Seq("C_ACCTBAL"),
    "part" -> Seq("P_RETAILPRICE"),
    "partsupp" -> Seq("PS_SUPPLYCOST"),
    "supplier" -> Seq("S_ACCTBAL"),
    "nation" -> Seq())

  /** The first element for each key, in input order. */
  def firstPer[T, K](xs: Seq[T])(key: T => K): Seq[T] =
    xs.foldLeft(Vector.empty[T]) { (acc, x) => if (acc.exists(a => key(a) == key(x))) acc else acc :+ x }

  def prefixOf(table: String): String = instantiation.dataset.tpch.TPCHSql.prefix(table).toUpperCase

  def tablePrefix: String = {
    val p = sys.env.getOrElse("DATAFARM_SQLSERVER_TABLE_PREFIX", "").trim
    if (p.isEmpty || p.endsWith(".")) p else p + "."
  }

  def fragment(entries: ListMap[String, Any]): String = "\n" + Marker + JsonUtil.toJson(entries) + "\n"
}
