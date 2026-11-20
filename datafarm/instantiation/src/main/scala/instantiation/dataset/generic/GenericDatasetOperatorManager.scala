package instantiation.dataset.generic

import instantiation.AbstractJob
import instantiation.dataset.{AbstractDatasetOperatorManager, AbstractTableOperatorManager, JoinRelation}
import instantiation.platform.flink.AbstractFlinkJob
import instantiation.platform.spark.AbstractSparkJob
import instantiation.platform.sql.AbstractSQLJob
import instantiation.platform.wayang.AbstractWayangJob

/**
  * Dataset manager for any dataset defined by a [[Catalog]] (datasets/<name>/catalog.json, see
  * datafarm/datasets/add_dataset.py). Generates jobs for every platform: Flink, Wayang, Spark, SQL and
  * SQL Server. The jobs read the dataset's files from their data path argument and parse them with a
  * parser object generated into each job ([[Parsers]]).
  */
class GenericDatasetOperatorManager(val catalog: Catalog, targetPlatform: String) extends AbstractDatasetOperatorManager {

  if (!GenericDatasetOperatorManager.Platforms.contains(targetPlatform))
    throw new Exception(s"""Dataset "${catalog.name}" supports TARGET_PLATFORM ${GenericDatasetOperatorManager.Platforms.mkString("\"", "\", \"", "\"")} (got "$targetPlatform").""" +
      (if (targetPlatform == "SQLServerFlat") " SQLServerFlat is TPC-H only." else ""))

  platform = targetPlatform

  val tables: Seq[String] = catalog.tables.map(_.name)

  def getTableOperatorManager(s: String, p: String): AbstractTableOperatorManager = {
    val t = catalog.table(s)
    p match {
      case "Flink" => new GenericFlinkTableOperatorManager(t, catalog.format)
      case "Wayang" => new GenericWayangTableOperatorManager(t, catalog.format)
      case "Spark" => new GenericSparkTableOperatorManager(t, catalog.format)
      case "SQL" => new GenericSQLTableOperatorManager(t, catalog.format)
      case "SQLServer" => new GenericSQLServerTableOperatorManager(t, catalog.format)
      case other => throw new Exception(s"Unknown platform '$other'")
    }
  }

  private def parserTables(tables: Seq[AbstractTableOperatorManager]): Seq[Table] =
    tables.map(_.tableName).distinct.map(catalog.table)

  override def flinkJob(jobBody: String, jobName: String, saveExecutionPlan: Boolean, tables: Seq[AbstractTableOperatorManager]): AbstractJob =
    AbstractFlinkJob(jobBody, jobName, saveExecutionPlan,
      Some(obj => Parsers.code(obj, catalog.format, parserTables(tables))), catalog.name)

  override def wayangJob(jobBody: String, jobName: String, saveExecutionPlan: Boolean, tables: Seq[AbstractTableOperatorManager]): AbstractJob =
    AbstractWayangJob(jobBody, jobName, obj => Parsers.code(obj, catalog.format, parserTables(tables)), catalog.name, saveExecutionPlan)

  override def sparkJob(jobBody: String, jobName: String, tables: Seq[AbstractTableOperatorManager]): AbstractJob =
    AbstractSparkJob(jobBody, jobName, obj => Parsers.code(obj, catalog.format, parserTables(tables)), catalog.name)

  override def sqlJob(jobBody: String, jobName: String, tables: Seq[AbstractTableOperatorManager]): AbstractJob =
    if (platform == "SQLServer") AbstractSQLJob(jobBody, jobName, s"${catalog.name} (SQL Server)", SchemaDDL.sqlServer(catalog))
    else AbstractSQLJob(jobBody, jobName, catalog.name, SchemaDDL.standard(catalog))

  /**
    * Join sequence for `nJoins` joins: a walk over the catalog's join graph where each join starts from
    * the table produced by the previous join and no join edge is used twice.
    *
    * A random walk gets stuck in lookup tables, whose only join leads back to where the walk came from.
    * Here the walk is a seeded, randomized depth-first search that backs out of dead ends, so long
    * sequences are found whenever they exist. A table is used at most as often as it occurs in a single
    * seed query (`maxOccurrences`); only if no such sequence exists is that limit dropped. The start table
    * is chosen by the seed; if no sequence exists from it, the other tables are tried.
    */
  override def getSourceTableSequence(nJoins: Int, seed: Int = 0): (Seq[AbstractTableOperatorManager], Seq[JoinRelation]) = {
    random.setSeed(seed)
    val candidates = if (nJoins == 0) tables else catalog.joinedTables
    if (candidates.isEmpty)
      throw new Exception(s"Dataset '${catalog.name}' has no join edges; jobs with $nJoins joins can not be generated")
    val first = candidates(Math.floorMod(seed, candidates.size))
    val starts = first +: random.shuffle(candidates.filterNot(_ == first))

    // (table, its join column, other table, other table's join column)
    type Edge = (String, String, String, String)
    def key(e: Edge): Set[(String, String)] = Set((e._1, e._2), (e._3, e._4))

    def find(capped: Boolean): Option[(String, List[Edge])] = {
      var budget = 0
      def allowed(t: String, count: Map[String, Int]) = !capped || count.getOrElse(t, 0) < catalog.table(t).maxOccurrences
      def search(cur: String, used: Set[Set[(String, String)]], count: Map[String, Int], acc: List[Edge]): Option[List[Edge]] =
        if (acc.size == nJoins) Some(acc.reverse)
        else if (budget <= 0) None
        else {
          budget -= 1
          random.shuffle(catalog.edgesFrom(cur).filter(e => !used.contains(key(e)) && allowed(e._3, count)))
            .iterator
            .map(e => search(e._3, used + key(e), count.updated(e._3, count.getOrElse(e._3, 0) + 1), e :: acc))
            .collectFirst { case Some(p) => p }
        }
      starts.iterator.map { s => budget = 50000; s -> search(s, Set(), Map(s -> 1), Nil) }.collectFirst { case (s, Some(p)) => (s, p) }
    }

    val (start, path) = find(capped = true).orElse {
      println(s"WARN - no $nJoins-join sequence within the table repetitions of the seed queries; allowing tables to repeat more often")
      find(capped = false)
    }.getOrElse(
      throw new Exception(s"No sequence of $nJoins joins without repeated join edges exists in the join graph of '${catalog.name}'"))

    val managers = (start +: path.map(_._3)).map(t => getTableOperatorManager(t, platform))
    val joins = path.zipWithIndex.map { case ((_, f, _, of), i) => JoinRelation(managers(i), managers(i + 1), of, f) }
    (managers, joins)
  }
}

object GenericDatasetOperatorManager {
  val Platforms: Seq[String] = Seq("Flink", "Wayang", "Spark", "SQL", "SQLServer")
}

/** CREATE TABLE statements of a catalog's tables for the generated SQL jobs (schema.sql next to the jobs). */
object SchemaDDL {

  private val VarChar = """(?i)(?:character\s+varying|varchar|nvarchar|char|character|nchar)\s*\(\s*(\d+)\s*\)""".r

  private def intType(c: Column, sqlServer: Boolean): String = {
    val t = c.sqlType.trim.toLowerCase
    if (t.startsWith("bigint") || t.startsWith("int8") || t.startsWith("bigserial")) "BIGINT"
    else if (t.startsWith("smallint") || t.startsWith("int2") || t.startsWith("tinyint")) "SMALLINT"
    else if (t.startsWith("int") || t.startsWith("serial") || t.startsWith("mediumint")) "INTEGER"
    else "BIGINT"
  }

  /** Text columns keep a declared length; CHAR(n) becomes VARCHAR(n), as the jobs do not pad; other types are text. */
  private def length(c: Column): Option[Int] = c.sqlType.trim match {
    case VarChar(n) if c.ordered => Some(n.toInt)
    case _ => None
  }

  private def ddl(catalog: Catalog, column: Column => String, comment: String, keyable: Column => Boolean): String = {
    val creates = catalog.tables.map { t =>
      val cols = t.columns.map(c => s"    ${c.name} ${column(c)}${if (!c.nullable) " NOT NULL" else ""}")
      val pkOk = t.primaryKey.nonEmpty && t.primaryKey.forall(k => keyable(t.column(k)))
      val pk = if (pkOk) s",\n    PRIMARY KEY (${t.primaryKey.mkString(", ")})" else ""
      s"CREATE TABLE ${t.name} (\n${cols.mkString(",\n")}$pk\n);"
    }
    s"""$comment
       |
       |${creates.mkString("\n\n")}
       |""".stripMargin
  }

  private def loadComment(catalog: Catalog, engines: String): String =
    s"""-- Tables of the ${catalog.name} dataset, used by the generated SQL jobs. Columns that are not integers
       |-- are text here, as in the dataflow jobs, so that all platforms compare them the same way.
       |-- Load the data files with
       |--   python datafarm/execution/sql/load_sql_data.py --engine $engines --dataset ${catalog.name} --data <data_dir> --schema schema.sql --connection "<connection>"
       |-- which parses them as the generated Flink, Wayang and Spark jobs do.""".stripMargin

  /** Standard SQL (PostgreSQL, DuckDB). */
  def standard(catalog: Catalog): String =
    ddl(catalog, c => if (c.isInt) intType(c, sqlServer = false) else length(c).map(n => s"VARCHAR($n)").getOrElse("TEXT"),
      loadComment(catalog, "postgres|duckdb"), _ => true)

  /**
    * SQL Server: text columns are NVARCHAR with a binary collation, so comparisons, LIKE and MIN are
    * case-sensitive and ordered by code point, as in the generated dataflow jobs. Primary keys on
    * NVARCHAR(MAX) or longer than 450 characters are left out (SQL Server can not index them).
    */
  def sqlServer(catalog: Catalog): String =
    ddl(catalog, c => if (c.isInt) intType(c, sqlServer = true)
    else s"NVARCHAR(${length(c).map(_.toString).getOrElse("MAX")}) COLLATE Latin1_General_100_BIN2",
      loadComment(catalog, "sqlserver"), c => c.isInt || length(c).exists(_ <= 450))
}
