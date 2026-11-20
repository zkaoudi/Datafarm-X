package instantiation.platform.sql

import instantiation.dataset.AbstractTableOperatorManager
import instantiation.dataset.JoinRelation
import instantiation.dataset.utils.getElementBySeed


/**
  * SQL code generation for a table: every abstract operator becomes one common table expression
  * (CTE), named like the variables of the dataflow jobs (`source0`, `filter1`, `join2`, ...), so a
  * job is one `WITH ... SELECT` statement with the same operators, in the same order, as the
  * Wayang, Flink and Spark jobs generated from the same plan:
  *
  *  - Data Source: `SELECT * FROM table`
  *  - Filter: `SELECT * FROM in WHERE condition`
  *  - Join: `SELECT r.* FROM lx l JOIN rx r ON l.a = r.b` (keeps the right-hand rows, like the dataflow jobs)
  *  - Group by / keyed Reduce: one row per key, the row the dataflow jobs keep: the smallest row
  *    comparing all columns in order, nulls first (`ROW_NUMBER() OVER (PARTITION BY key ORDER BY c1, c2, ...) = 1`)
  *  - global Reduce: the smallest row of the input (`ROW_NUMBER() OVER (ORDER BY c1, c2, ...) = 1`; no row for empty input)
  *  - Sort-Partition: `SELECT * FROM in ORDER BY column`
  *  - Map, Partition: pass-through (`SELECT * FROM in`); their UDF cost has no SQL counterpart
  *
  * The SQL follows [[sqlDialect]]: standard SQL by default (tested with PostgreSQL and DuckDB), or
  * T-SQL for SQL Server. Columns are named by [[sqlColumn]].
  */
trait AbstractSQLTableOperatorManager extends AbstractTableOperatorManager {

  // ---- building blocks (also used by dataset-specific managers) -----------------------------

  /** The SQL dialect to generate (a def, so it is not part of the job info JSON). */
  def sqlDialect: SQLDialect = SQLDialect.Standard

  /** SQL table name. */
  def sqlTable: String = tableName

  /** SQL columns of this table, in tuple order. */
  def sqlColumns: Seq[String] = fields.toSeq.sortBy(_._2.toInt).map(f => sqlColumn(f._1))

  def sqlCte(name: String, query: String): String = AbstractSQLJob.cte(name, query)

  def sqlSourceCode(outVarName: String): String = sqlCte(outVarName, s"SELECT * FROM $sqlTable")

  def sqlPassThroughCode(inVarName: String, outVarName: String): String = sqlCte(outVarName, s"SELECT * FROM $inVarName")

  def sqlFilterCode(inVarName: String, outVarName: String, condition: String): String =
    sqlCte(outVarName, s"SELECT * FROM $inVarName WHERE $condition")

  /**
    * The smallest row of each partition (or of the whole input), comparing all columns in order with
    * NULLs first: the row the dataflow jobs keep with RecordOrder's minRecord. Text columns are
    * compared with the database's collation, which can differ from Java's order only between rows
    * that agree on all earlier columns (TPC-H and JOB rows start with their key columns).
    */
  private def sqlFirstRowCode(inVarName: String, outVarName: String, partitionBy: Option[String]): String = {
    val cols = sqlColumns
    val over = partitionBy.map(k => s"PARTITION BY $k ").getOrElse("") +
      "ORDER BY " + cols.map(sqlDialect.ascNullsFirst).mkString(", ")
    sqlCte(outVarName,
      s"SELECT ${cols.mkString(", ")} FROM (SELECT *, ROW_NUMBER() OVER ($over) AS datafarm_rn FROM $inVarName) ranked " +
        "WHERE datafarm_rn = 1")
  }

  /** One row per value of `keyExpr` (normally the column `keyColumn` itself): its smallest row. */
  def sqlKeyedReduceCode(inVarName: String, outVarName: String, keyColumn: String, keyExpr: String): String =
    sqlFirstRowCode(inVarName, outVarName, Some(keyExpr))

  /** The smallest row of the input; no row for an empty input. */
  def sqlGlobalReduceCode(inVarName: String, outVarName: String): String =
    sqlFirstRowCode(inVarName, outVarName, None)

  def sqlSortCode(inVarName: String, outVarName: String, column: String): String =
    sqlCte(outVarName, s"SELECT * FROM $inVarName ${sqlDialect.orderByInCte(column)}")

  private def fieldName(index: String): String =
    fields.find(_._2 == index).map(_._1).getOrElse(throw new Exception(s"No field at position $index in table '$tableName'"))

  // ---- operators ----------------------------------------------------------------------------

  override def dataSourceCode(outVarName: String): String = sqlSourceCode(outVarName)

  override def buildDataSourceCode(schema: String, tableName: String, outVarName: String, delimiter: String = "|"): String =
    sqlSourceCode(outVarName)

  /** `filterField` is the 1-based field position, `filterValue` a Scala literal as written by the table managers. */
  override def buildFilterCode(inVarName: String, outVarName: String, filterField: String, filterValue: String, filterOp: String = "<="): String = {
    val column = sqlColumn(fieldName(filterField))
    val value = filterValue.trim match {
      case AbstractTableOperatorManager.ParseDate(date) => sqlDialect.dateLiteral(date)
      case v if v.startsWith("\"") && v.endsWith("\"") => "'" + v.substring(1, v.length - 1).replace("'", "''") + "'"
      case v => v
    }
    val op = filterOp match {
      case "==" => "="
      case "!=" => "<>"
      case o => o
    }
    sqlFilterCode(inVarName, outVarName, s"$column $op $value")
  }

  override def buildJoinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String = {
    val lxColumn = joinRelation.lx.sqlColumn(joinRelation.lxField)
    val rxColumn = joinRelation.rx.sqlColumn(joinRelation.field)
    sqlCte(outVarName, s"SELECT r.* FROM $lxVarName l JOIN $rxVarName r ON l.$lxColumn = r.$rxColumn")
  }

  override def joinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String =
    buildJoinCode(lxVarName, rxVarName, outVarName, joinRelation)

  override def mapCode(inVarName: String, outVarName: String, seed: Int = 0): (String, Int) =
    (sqlPassThroughCode(inVarName, outVarName), seed % 3)

  override def groupByCode1(inVarName: String, outVarName: String, seed: Int): (String, Double) = {
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val c = sqlColumn(gField)
    (sqlKeyedReduceCode(inVarName, outVarName, c, c), groupFields(gField).toDouble)
  }

  override def groupByCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): (String, Double) = {
    // two consecutive group-bys collapse into one, as for the dataflow platforms
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val c = sqlColumn(gField)
    (sqlKeyedReduceCode(inVarName, outVarName2, c, c), groupFields(gField).toDouble)
  }

  override def reduceCode1(inVarName: String, outVarName: String): String = sqlGlobalReduceCode(inVarName, outVarName)

  /** A Reduce pair (combiner + reduce of a grouped reduce in the input workload) on a group field. */
  override def reduceCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): String = {
    val rField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val c = sqlColumn(rField)
    sqlKeyedReduceCode(inVarName, outVarName2, c, c)
  }

  override def sortPartitionCode(inVarName: String, outVarName: String, seed: Int): String = {
    val sField = getElementBySeed(fields.keys.toSeq, seed).toString
    sqlSortCode(inVarName, outVarName, sqlColumn(sField))
  }

  override def partitionCode(inVarName: String, outVarName: String): String = sqlPassThroughCode(inVarName, outVarName)

  override def sinkCode(inVarName: String, outVarName: String): String = AbstractSQLJob.sink(inVarName)
}
