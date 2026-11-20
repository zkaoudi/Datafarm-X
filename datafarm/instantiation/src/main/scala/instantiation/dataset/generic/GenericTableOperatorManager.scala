package instantiation.dataset.generic

import instantiation.dataset.{AbstractTableOperatorManager, JoinRelation, ParserCode, RecordOrder}
import instantiation.platform.spark.AbstractSparkTableOperatorManager
import instantiation.platform.sql.{AbstractSQLTableOperatorManager, SQLDialect}
import instantiation.platform.wayang.AbstractWayangTableOperatorManager

import com.fasterxml.jackson.annotation.JsonIgnore

import scala.annotation.meta.{field, getter, param}
import scala.collection.immutable.ListMap

/**
  * The dataset-independent part of operator generation for one table of a [[Catalog]]: reading and
  * parsing the data file, filter predicates, group-by and sort keys that are safe for nullable columns,
  * and joins that, as in SQL, never match NULL keys.
  *
  * Mixed into one class per platform ([[GenericFlinkTableOperatorManager]], [[GenericWayangTableOperatorManager]],
  * [[GenericSparkTableOperatorManager]], [[GenericSQLTableOperatorManager]] and
  * [[GenericSQLServerTableOperatorManager]]), which supply the platform's code through the hooks below.
  *
  * Semantics shared by all platforms (and by the SQL jobs):
  *  - Filter: the predicate, NULL never satisfies it (except `is null`);
  *  - Join: equi-join keeping the right-hand row; NULL keys match nothing;
  *  - Group by: one row per key, the smallest row (RecordOrder.minRecord); NULL text keys group with "";
  *  - Reduce: the smallest row of the whole input;
  *  - Sort-Partition: sort on a column without NULLs.
  */
trait GenericTableLogic extends AbstractTableOperatorManager {

  // JobInfoRecorder serializes table managers: the table definition is left out of the job info JSON
  def table: Table

  def format: DataFormat

  // ---- platform hooks ----------------------------------------------------------------------

  /** Read `file` from the data path, skip `header` if given, and turn each line into a tuple with `parser`. */
  protected def genericSourceCode(outVarName: String, file: String, parser: String, name: String, header: Option[String]): String

  /** Keep one element per value of the key column `field`. */
  protected def genericKeyedReduceCode(inVarName: String, outVarName: String, field: String): String

  /** Reduce the whole input to a single element. */
  protected def genericGlobalReduceCode(inVarName: String, outVarName: String): String

  /** Sort by the tuple field at 1-based position `index`. */
  protected def genericSortCode(inVarName: String, outVarName: String, index: Int): String

  /** Equi-join `lx._lxIndex == rx._rxIndex`, keeping the right-hand tuple; both inputs are free of NULL keys. */
  protected def genericJoinCode(lxVarName: String, rxVarName: String, outVarName: String, lxIndex: Int, rxIndex: Int, keyType: String): String

  /** Keep the rows that satisfy `p`; the default is the dataflow form shared by Flink, Wayang and Spark. */
  protected def genericFilterCode(inVarName: String, outVarName: String, p: Predicate): String =
    s"""
       |val $outVarName = $inVarName.filter(x => ${p.condition(ParserCode.Token)})
      """.stripMargin

  // ---- table description -------------------------------------------------------------------

  override val tableName: String = table.name

  override val wayangName: String = table.name

  override val typeSchema: String = table.tupleType

  override val fields: Map[String, String] =
    ListMap(table.columns.zipWithIndex.map { case (c, i) => c.name -> (i + 1).toString }: _*)

  /** column -> (other table -> other table's column); informational (the join walk uses the catalog's edges). */
  override val joinFieldTable: Map[String, Map[String, String]] = Map()

  private val predicates: Seq[Predicate] = table.filters

  /** field -> predicates (as SQL) and their selectivities ("-1" = not measured); informational. */
  override val filterFieldValue: Map[String, Map[String, Seq[String]]] =
    ListMap(predicates.map(_.field).distinct.map { f =>
      val ps = predicates.filter(_.field == f)
      f -> Map("predicates" -> ps.map(_.sql), "selectivity" -> ps.map(p => p.selectivity.map(_.toString).getOrElse("-1")))
    }: _*)

  /** group field -> number of distinct values ("-1" = not measured). */
  override val groupFields: Map[String, String] =
    ListMap(table.groupFields.map(g => g.field -> g.distinct.map(_.toString).getOrElse("-1")): _*)

  // ---- data source -------------------------------------------------------------------------

  override def dataSourceCode(outVarName: String): String =
    genericSourceCode(outVarName, table.file, s"${ParserCode.Token}.${table.name}", table.name,
      if (format.header) table.header else None)

  // ---- filter ------------------------------------------------------------------------------

  override def filterCode(inVarName: String, outVarName: String, fieldSeed: Int = 0, valueSeed: Int = 0): (String, Double) = {
    val p = predicates(Math.floorMod(fieldSeed, predicates.size))
    (genericFilterCode(inVarName, outVarName, p), p.selectivity.getOrElse(-1.0))
  }

  // ---- join --------------------------------------------------------------------------------

  /** Code that drops the tuples whose `column` is NULL, or "" if it is not nullable. */
  protected def notNullFilter(column: Column, index: Int): String =
    if (!column.nullable) "" else s".filter(x => !(${column.nullTest(s"x._$index")}))"

  override def joinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String = {
    val lt = GenericTableLogic.tableOf(joinRelation.lx)
    val rt = GenericTableLogic.tableOf(joinRelation.rx)
    val (lc, rc) = (lt.column(joinRelation.lxField), rt.column(joinRelation.field))
    val (li, ri) = (lt.index(lc.name), rt.index(rc.name))
    val lxKeys = s"${outVarName}l"
    val rxKeys = s"${outVarName}r"
    s"""
       |val $lxKeys = $lxVarName${notNullFilter(lc, li)}
       |val $rxKeys = $rxVarName${notNullFilter(rc, ri)}
      """.stripMargin +
      genericJoinCode(lxKeys, rxKeys, outVarName, li, ri, lc.scalaType)
  }

  override def buildJoinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String =
    joinCode(lxVarName, rxVarName, outVarName, joinRelation)

  // ---- group by ----------------------------------------------------------------------------

  /** Dataflow key extractor for a group field; NULL text values are grouped under "" (null keys are rejected). */
  protected def keyUdf(field: String): String = {
    val c = table.column(field)
    val i = table.index(field)
    if (c.isText && c.nullable) s"""(x => if (x._$i == null) "" else x._$i)""" else s"(_._$i)"
  }

  private def groupField(seed: Int): GroupField = table.groupFields(Math.floorMod(seed, table.groupFields.size))

  private def distinct(g: GroupField): Double = g.distinct.map(_.toDouble).getOrElse(-1.0)

  override def groupByCode1(inVarName: String, outVarName: String, seed: Int): (String, Double) = {
    val g = groupField(seed)
    (genericKeyedReduceCode(inVarName, outVarName, g.field), distinct(g))
  }

  override def groupByCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): (String, Double) = {
    // two consecutive group-bys (combiner + reduce of a GROUP BY) collapse into one keyed reduce
    val g = groupField(seed)
    (genericKeyedReduceCode(inVarName, outVarName2, g.field), distinct(g))
  }

  // ---- reduce ------------------------------------------------------------------------------

  // A Reduce pair is the combiner + reduce of an aggregate without GROUP BY: one row for the whole input.
  override def reduceCode1(inVarName: String, outVarName: String): String = genericGlobalReduceCode(inVarName, outVarName)

  override def reduceCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): String =
    genericGlobalReduceCode(inVarName, outVarName2)

  // ---- sort --------------------------------------------------------------------------------

  override def sortPartitionCode(inVarName: String, outVarName: String, seed: Int): String = {
    val keys = table.nullSafeKeyColumns
    val c = keys(Math.floorMod(seed, keys.size))
    genericSortCode(inVarName, outVarName, table.index(c.name))
  }
}

object GenericTableLogic {

  /** `val out = start` followed by one chained call per line. */
  def chain(outVarName: String, start: String, calls: Seq[String]): String =
    (s"\nval $outVarName = $start" +: calls.map("  " + _)).mkString("\n") + "\n"

  def tableOf(m: AbstractTableOperatorManager): Table = m match {
    case g: GenericTableLogic => g.table
    case other => throw new Exception(s"Table manager of '${other.tableName}' is not one of a generic dataset")
  }
}

/** Generic dataset table on Flink (DataSet API). */
class GenericFlinkTableOperatorManager(@(JsonIgnore @param @field @getter) val table: Table,
                                       @(JsonIgnore @param @field @getter) val format: DataFormat)
  extends AbstractTableOperatorManager with GenericTableLogic {

  protected def genericSourceCode(outVarName: String, file: String, parser: String, name: String, header: Option[String]): String =
    GenericTableLogic.chain(outVarName, s"""env.readTextFile(params("dataPath") + "$file")""",
      header.map(h => s".filter(line => line != ${Predicate.lit(h)})").toSeq ++
        Seq(s".map(line => $parser(line))", s""".name("Read $name")"""))

  protected def genericKeyedReduceCode(inVarName: String, outVarName: String, field: String): String =
    s"""
       |val $outVarName = $inVarName.groupBy${keyUdf(field)}.reduce(${RecordOrder.udf(ParserCode.Token)})
      """.stripMargin

  protected def genericGlobalReduceCode(inVarName: String, outVarName: String): String =
    s"""
       |val $outVarName = $inVarName.reduce(${RecordOrder.udf(ParserCode.Token)})
      """.stripMargin

  protected def genericSortCode(inVarName: String, outVarName: String, index: Int): String =
    s"""
       |val $outVarName = $inVarName.sortPartition(_._$index, Order.ASCENDING)
      """.stripMargin

  // apply(...) and not a block after equalTo(...), which would be taken as equalTo's implicit TypeInformation
  protected def genericJoinCode(lxVarName: String, rxVarName: String, outVarName: String, lxIndex: Int, rxIndex: Int, keyType: String): String =
    s"""
       |val $outVarName = $lxVarName.join($rxVarName).where(_._$lxIndex).equalTo(_._$rxIndex).apply((lx, rx) => rx)
      """.stripMargin

  override def partitionCode(inVarName: String, outVarName: String): String =
    s"""
       |val $outVarName = $inVarName.partitionByHash(_._1)
      """.stripMargin

  /** Counts the rows into an accumulator, which the job reports after its execution. */
  override def sinkCode(inVarName: String, outVarName: String): String =
    s"""
       |val $outVarName = $inVarName.output(new org.apache.flink.api.java.Utils.CountHelper(RowsAccumulator))
      """.stripMargin
}

/** Generic dataset table on Wayang. */
class GenericWayangTableOperatorManager(@(JsonIgnore @param @field @getter) val table: Table,
                                        @(JsonIgnore @param @field @getter) val format: DataFormat)
  extends AbstractWayangTableOperatorManager with GenericTableLogic {

  protected def genericSourceCode(outVarName: String, file: String, parser: String, name: String, header: Option[String]): String =
    GenericTableLogic.chain(outVarName, "planBuilder",
      Seq(s""".readTextFile(datapath + "$file")""", s""".withName("Read $name")""") ++
        header.map(h => s".filter(line => line != ${Predicate.lit(h)}).withName(${Predicate.lit("Skip header of " + name)})").toSeq ++
        Seq(s".map($parser)", s""".withName("Parse $name to tuple")"""))

  protected def genericKeyedReduceCode(inVarName: String, outVarName: String, field: String): String =
    s"""
        |val $outVarName = $inVarName.reduceByKey(${keyUdf(field)}, ${RecordOrder.udf(ParserCode.Token)})
      """.stripMargin

  protected def genericGlobalReduceCode(inVarName: String, outVarName: String): String =
    s"""
       |val $outVarName = $inVarName.reduce(${RecordOrder.udf(ParserCode.Token)})
      """.stripMargin

  protected def genericSortCode(inVarName: String, outVarName: String, index: Int): String =
    s"""
       |val $outVarName = $inVarName.sort(_._$index)
      """.stripMargin

  protected def genericJoinCode(lxVarName: String, rxVarName: String, outVarName: String, lxIndex: Int, rxIndex: Int, keyType: String): String =
    s"""
       |val $outVarName = $lxVarName.keyBy[$keyType](_._$lxIndex).join($rxVarName.keyBy[$keyType](_._$rxIndex)).map(x => x.field1)
      """.stripMargin
}

/** Generic dataset table on Spark (RDD API). */
class GenericSparkTableOperatorManager(@(JsonIgnore @param @field @getter) val table: Table,
                                       @(JsonIgnore @param @field @getter) val format: DataFormat)
  extends AbstractSparkTableOperatorManager with GenericTableLogic {

  protected def genericSourceCode(outVarName: String, file: String, parser: String, name: String, header: Option[String]): String =
    GenericTableLogic.chain(outVarName, s"""sc.textFile(datapath + "$file")""",
      Seq(s""".setName("Read $name")""") ++
        header.map(h => s".filter(line => line != ${Predicate.lit(h)})").toSeq ++
        Seq(s".map($parser)", s""".setName("Parse $name to tuple")"""))

  protected def genericKeyedReduceCode(inVarName: String, outVarName: String, field: String): String =
    sparkKeyedReduceCode(inVarName, outVarName, keyUdf(field))

  protected def genericGlobalReduceCode(inVarName: String, outVarName: String): String =
    sparkGlobalReduceCode(inVarName, outVarName)

  protected def genericSortCode(inVarName: String, outVarName: String, index: Int): String =
    sparkSortCode(inVarName, outVarName, index)

  protected def genericJoinCode(lxVarName: String, rxVarName: String, outVarName: String, lxIndex: Int, rxIndex: Int, keyType: String): String =
    s"""
       |val $outVarName = $lxVarName.keyBy(_._$lxIndex).join($rxVarName.keyBy(_._$rxIndex)).map(_._2._2)
       """.stripMargin
}

/** Generic dataset table as SQL (one CTE per operator, see [[AbstractSQLTableOperatorManager]]). */
class GenericSQLTableOperatorManager(@(JsonIgnore @param @field @getter) val table: Table,
                                     @(JsonIgnore @param @field @getter) val format: DataFormat)
  extends AbstractSQLTableOperatorManager with GenericTableLogic {

  protected def genericSourceCode(outVarName: String, file: String, parser: String, name: String, header: Option[String]): String =
    sqlCte(outVarName, s"SELECT * FROM $name")

  /** NULL text keys are grouped together with "", as in the dataflow jobs. */
  protected def genericKeyedReduceCode(inVarName: String, outVarName: String, field: String): String = {
    val c = table.column(field)
    val keyExpr = if (c.isText && c.nullable) s"COALESCE($field, '')" else field
    sqlKeyedReduceCode(inVarName, outVarName, field, keyExpr)
  }

  protected def genericGlobalReduceCode(inVarName: String, outVarName: String): String =
    sqlGlobalReduceCode(inVarName, outVarName)

  protected def genericSortCode(inVarName: String, outVarName: String, index: Int): String =
    sqlSortCode(inVarName, outVarName, table.columns(index - 1).name)

  override protected def genericFilterCode(inVarName: String, outVarName: String, p: Predicate): String =
    sqlFilterCode(inVarName, outVarName, p.sqlCondition(sqlDialect))

  /** NULL keys never join in SQL, so no NULL filter is needed. */
  override def joinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String = {
    val lc = joinRelation.lx.sqlColumn(joinRelation.lxField)
    val rc = joinRelation.rx.sqlColumn(joinRelation.field)
    sqlCte(outVarName, s"SELECT r.* FROM $lxVarName l JOIN $rxVarName r ON l.$lc = r.$rc")
  }

  protected def genericJoinCode(lxVarName: String, rxVarName: String, outVarName: String, lxIndex: Int, rxIndex: Int, keyType: String): String =
    throw new UnsupportedOperationException("SQL joins are generated by joinCode")
}

/** Generic dataset table as T-SQL for SQL Server. */
class GenericSQLServerTableOperatorManager(t: Table, f: DataFormat) extends GenericSQLTableOperatorManager(t, f) {
  override def sqlDialect: SQLDialect = SQLDialect.SQLServer
}
