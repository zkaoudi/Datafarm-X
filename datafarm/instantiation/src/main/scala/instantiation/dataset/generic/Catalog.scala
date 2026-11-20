package instantiation.dataset.generic

import java.io.{File, FileInputStream}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.collection.JavaConverters._

/**
  * A column of a dataset table. In the generated jobs every row is a Scala tuple in column order, with
  * values by the column's kind (see datafarm/datasets/csvformat.py, which reads them the same way):
  *  - `int`: `Long`, NULL is `Long.MinValue`;
  *  - `decimal`: `Double`, NULL is `NaN`;
  *  - `date`: `Long` days since 1970-01-01, NULL is `Long.MinValue`;
  *  - `timestamp`: `Long` milliseconds since 1970-01-01 00:00, NULL is `Long.MinValue`;
  *  - `text`: `String`, NULL is null.
  *
  * @param sqlType the type declared in the dataset's schema.sql
  * @param ordered for text columns, whether their text orders like their values (false for types such as
  *                BOOLEAN or UUID, which are read as text and only compared for equality)
  */
case class Column(name: String, kind: String, nullable: Boolean, sqlType: String, ordered: Boolean) {
  require(Column.Kinds.contains(kind), s"Column '$name': unknown kind '$kind' (expected ${Column.Kinds.mkString(", ")})")

  def isInt: Boolean = kind == "int"
  def isText: Boolean = kind == "text"
  def isDecimal: Boolean = kind == "decimal"
  /** int, date and timestamp values are Longs */
  def isLong: Boolean = kind == "int" || kind == "date" || kind == "timestamp"

  def scalaType: String = if (isLong) "Long" else if (isDecimal) "Double" else "String"

  /** Scala expression that is true when tuple field `v` is NULL. */
  def nullTest(v: String): String = if (isLong) s"$v == Long.MinValue" else if (isDecimal) s"$v.isNaN" else s"$v == null"
}

object Column {
  val Kinds: Seq[String] = Seq("int", "decimal", "date", "timestamp", "text")
}

/** How the data files are read (see datafarm/datasets/csvformat.py, which implements the same rules). */
case class DataFormat(delimiter: Char, quote: Option[Char], escape: Option[Char], nullValue: String, header: Boolean)

case class GroupField(field: String, distinct: Option[Long])

/**
  * A table of a dataset catalog.
  *
  * @param file           name of the data file in the job's data path
  * @param header         the header line the jobs skip, if the files have one
  * @param groupFields    columns used for generated group-by / reduce-by-key operators
  * @param filters        the filter predicates offered for this table
  * @param maxOccurrences how often the table occurs at most in one seed query (bounds the join walk)
  */
case class Table(name: String, file: String, header: Option[String], rows: Option[Long], columns: Seq[Column],
                 primaryKey: Seq[String], groupFields: Seq[GroupField], filters: Seq[Predicate], maxOccurrences: Int) {

  require(columns.size <= 22, s"Table '$name' has more columns than a Scala tuple can hold")
  require(groupFields.nonEmpty, s"Table '$name' needs at least one group field")
  require(filters.nonEmpty, s"Table '$name' needs at least one filter predicate")
  groupFields.foreach(g => require(columns.exists(_.name == g.field), s"Unknown group field '$name.${g.field}'"))

  def column(col: String): Column =
    columns.find(_.name == col).getOrElse(throw new Exception(s"Table '$name' has no column '$col'"))

  /** 1-based position of the column, i.e. the tuple accessor `_i`. */
  def index(col: String): Int = {
    val i = columns.indexWhere(_.name == col)
    if (i < 0) throw new Exception(s"Table '$name' has no column '$col'")
    i + 1
  }

  def tupleType: String = columns.map(_.scalaType).mkString("(", ", ", ")")

  /** Columns that can be used as sort keys without null handling. */
  def nullSafeKeyColumns: Seq[Column] = {
    val safe = columns.filter(c => c.isLong || !c.nullable)
    if (safe.nonEmpty) safe else columns.take(1)
  }
}

/** An equi-join `table1.column1 = table2.column2` of the join graph. */
case class JoinEdge(table1: String, column1: String, table2: String, column2: String, queries: Int)

/**
  * A dataset defined by a folder `datasets/<name>/` (schema.sql, seed queries, data) and described by the
  * `catalog.json` that datafarm/datasets/add_dataset.py derives from it: tables, columns, join graph,
  * filter candidates, group fields and statistics.
  */
case class Catalog(name: String, about: String, format: DataFormat, tables: Seq[Table], joins: Seq[JoinEdge], origin: String) {

  private val byName: Map[String, Table] = tables.map(t => t.name -> t).toMap

  def table(t: String): Table = byName.getOrElse(t, throw new Exception(s"Dataset '$name' has no table '$t'"))

  /** The join edges at table `t` as (t, its column, other table, other table's column), both directions. */
  def edgesFrom(t: String): Seq[(String, String, String, String)] =
    joins.flatMap { e =>
      (if (e.table1 == t) Seq((t, e.column1, e.table2, e.column2)) else Nil) ++
        (if (e.table2 == t) Seq((t, e.column2, e.table1, e.column1)) else Nil)
    }

  /** Tables with at least one join edge: the only ones a job with joins can start from. */
  val joinedTables: Seq[String] = tables.map(_.name).filter(t => edgesFrom(t).nonEmpty)

  joins.foreach { e =>
    val (c1, c2) = (table(e.table1).column(e.column1), table(e.table2).column(e.column2))
    require(c1.kind == c2.kind, s"Join ${e.table1}.${e.column1} = ${e.table2}.${e.column2} needs columns of the same kind")
  }
}

object Catalog {

  /** Default folder of the dataset definitions, relative to the instantiation project (where sbt runs). */
  val DefaultRoot = "../../datasets"
  val RootEnvVar = "DATAFARM_DATASETS"

  /**
    * The catalog of a dataset given as a path to its catalog.json, its folder, or its name (a folder in
    * `$DATAFARM_DATASETS`, by default datasets/ of the repository; names are matched case-insensitively).
    */
  def resolve(dataset: String): File = {
    val root = new File(sys.env.getOrElse(RootEnvVar, DefaultRoot))
    val direct = new File(dataset)
    val candidates =
      Seq(direct, new File(direct, "catalog.json"), new File(new File(root, dataset), "catalog.json"),
        new File(new File(root, dataset.toLowerCase), "catalog.json"))
    candidates.find(f => f.isFile).getOrElse(throw new Exception(
      s"Can not find dataset '$dataset': no catalog.json in ${candidates.map(_.getPath).mkString(", ")}. " +
        "Create it with: python -m datafarm.datasets.add_dataset datasets/<name>"))
  }

  def load(dataset: String): Catalog = {
    val file = resolve(dataset)
    val in = new FileInputStream(file)
    try parse(new ObjectMapper().readTree(in), file.getPath) finally in.close()
  }

  def parse(root: JsonNode, origin: String): Catalog = {
    def fail(msg: String) = throw new Exception(s"Catalog $origin: $msg")
    def text(n: JsonNode, key: String): String =
      Option(n.get(key)).filterNot(_.isNull).map(_.asText()).getOrElse(fail(s"missing '$key'"))
    def optText(n: JsonNode, key: String): Option[String] = Option(n.get(key)).filterNot(_.isNull).map(_.asText())
    def optLong(n: JsonNode, key: String): Option[Long] = Option(n.get(key)).filterNot(_.isNull).map(_.asLong())
    def optChar(n: JsonNode, key: String): Option[Char] = optText(n, key).map { s =>
      if (s.length != 1) fail(s"format.$key must be one character, not '$s'")
      s.charAt(0)
    }
    def elements(n: JsonNode, key: String): Seq[JsonNode] =
      Option(n.get(key)).filterNot(_.isNull).map(_.elements().asScala.toSeq).getOrElse(Nil)

    val f = Option(root.get("format")).getOrElse(fail("missing 'format'"))
    val format = DataFormat(
      optChar(f, "delimiter").getOrElse(','), optChar(f, "quote"), optChar(f, "escape"),
      optText(f, "null").getOrElse(""), Option(f.get("header")).exists(_.asBoolean(false)))

    val tables = elements(root, "tables").map { t =>
      val tn = text(t, "name")
      val columns = elements(t, "columns").map { c =>
        val tp = text(c, "type")
        if (!Column.Kinds.contains(tp)) fail(s"column $tn.${text(c, "name")}: type must be one of ${Column.Kinds.mkString(", ")}, not '$tp'")
        Column(text(c, "name"), tp, Option(c.get("nullable")).forall(_.asBoolean(true)),
          optText(c, "sqlType").getOrElse(tp),
          Option(c.get("ordered")).forall(_.asBoolean(true)))
      }
      val groupFields = elements(t, "groupFields").map(g => GroupField(text(g, "field"), optLong(g, "distinct")))
      val filters = elements(t, "filters").map { p =>
        val values = Option(p.get("value")) match {
          case None => Seq()
          case Some(n) if n.isNull => Seq()
          case Some(n) if n.isArray => n.elements().asScala.map(_.asText()).toSeq
          case Some(n) => Seq(n.asText())
        }
        val sel = Option(p.get("selectivity")).filterNot(_.isNull).map(_.asDouble())
        (text(p, "field"), text(p, "op").toLowerCase, values, sel)
      }
      // predicates check their column, so they are built against the table's columns
      val proto = TableColumns(tn, columns)
      val predicates = filters.map { case (field, op, values, sel) => Predicate(proto, field, op, values, sel) }
      val header = optText(t, "header")
      if (format.header && header.isEmpty)
        fail(s"format.header is set, but the header line of table $tn is unknown (run add_dataset.py with --data)")
      Table(tn, optText(t, "file").getOrElse(tn + ".csv"), header, optLong(t, "rows"), columns,
        elements(t, "primaryKey").map(_.asText()), groupFields, predicates,
        Option(t.get("maxOccurrences")).filterNot(_.isNull).map(_.asInt()).getOrElse(1))
    }
    if (tables.isEmpty) fail("no tables")

    val joins = elements(root, "joins").map { j =>
      def side(key: String) = text(j, key).split('.') match {
        case Array(t, c) => (t, c)
        case _ => fail(s"join side '${text(j, key)}' is not table.column")
      }
      val ((t1, c1), (t2, c2)) = (side("left"), side("right"))
      JoinEdge(t1, c1, t2, c2, Option(j.get("queries")).filterNot(_.isNull).map(_.asInt()).getOrElse(0))
    }
    Catalog(optText(root, "name").getOrElse(new File(origin).getParentFile.getName),
      optText(root, "about").getOrElse(""), format, tables, joins, origin)
  }
}

/** The columns of a table, which a [[Predicate]] needs to check and address its column. */
case class TableColumns(name: String, columns: Seq[Column]) {
  def column(col: String): Column =
    columns.find(_.name == col).getOrElse(throw new Exception(s"Table '$name' has no column '$col'"))

  def index(col: String): Int = columns.indexWhere(_.name == col) + 1
}
