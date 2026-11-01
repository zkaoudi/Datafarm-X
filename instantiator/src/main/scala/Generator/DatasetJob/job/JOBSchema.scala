package Generator.DatasetJob.job

import scala.collection.immutable.ListMap

/**
  * Schema of the IMDB database used by the Join Order Benchmark (JOB).
  *
  * V. Leis, A. Gubichev, A. Mirchev, P. Boncz, A. Kemper, T. Neumann.
  * How Good Are Query Optimizers, Really? PVLDB 9(3), 2015.
  *
  * Tables and columns follow `schema.sql` of https://github.com/gregrahn/join-order-benchmark.
  * The data is expected as the 21 CSV files of the original `imdb.tgz` dump (`title.csv`,
  * `cast_info.csv`, ...): comma separated, text quoted with `"`, `\` as escape character,
  * NULL written as an empty unquoted field.
  *
  * In the generated Wayang jobs every row becomes a Scala tuple in column order:
  * integer columns are `Long` (NULL -> `Long.MinValue`), text columns are `String` (NULL -> null).
  */
case class JOBColumn(name: String, isInt: Boolean, nullable: Boolean) {
  def scalaType: String = if (isInt) "Long" else "String"
}

/**
  * @param groupFields columns used for generated group-by/reduce-by-key operators
  */
case class JOBTable(name: String, columns: Seq[JOBColumn], groupFields: Seq[String]) {

  require(columns.size <= 22, s"Table '$name' has more columns than a Scala tuple can hold")
  require(groupFields.nonEmpty, s"Table '$name' needs at least one group field")
  groupFields.foreach(g => require(columns.exists(_.name == g), s"Unknown group field '$name.$g'"))

  def fileName: String = s"$name.csv"

  def column(col: String): JOBColumn =
    columns.find(_.name == col).getOrElse(throw new Exception(s"Table '$name' has no column '$col'"))

  /** 1-based position of the column, i.e. the tuple accessor `_i`. */
  def index(col: String): Int = {
    val i = columns.indexWhere(_.name == col)
    if (i < 0) throw new Exception(s"Table '$name' has no column '$col'")
    i + 1
  }

  def tupleType: String = columns.map(_.scalaType).mkString("(", ", ", ")")

  /** Columns that can be used as sort/group keys without null handling. */
  def nullSafeKeyColumns: Seq[JOBColumn] = columns.filter(c => c.isInt || !c.nullable)
}

object JOBSchema {

  private def int(name: String, nullable: Boolean = false) = JOBColumn(name, isInt = true, nullable)

  private def text(name: String, nullable: Boolean = false) = JOBColumn(name, isInt = false, nullable)

  val tables: Seq[JOBTable] = Seq(
    JOBTable("aka_name", Seq(
      int("id"), int("person_id"), text("name"), text("imdb_index", nullable = true),
      text("name_pcode_cf", nullable = true), text("name_pcode_nf", nullable = true),
      text("surname_pcode", nullable = true), text("md5sum", nullable = true)),
      groupFields = Seq("person_id")),

    JOBTable("aka_title", Seq(
      int("id"), int("movie_id"), text("title"), text("imdb_index", nullable = true), int("kind_id"),
      int("production_year", nullable = true), text("phonetic_code", nullable = true),
      int("episode_of_id", nullable = true), int("season_nr", nullable = true),
      int("episode_nr", nullable = true), text("note", nullable = true), text("md5sum", nullable = true)),
      groupFields = Seq("movie_id", "kind_id", "production_year")),

    JOBTable("cast_info", Seq(
      int("id"), int("person_id"), int("movie_id"), int("person_role_id", nullable = true),
      text("note", nullable = true), int("nr_order", nullable = true), int("role_id")),
      groupFields = Seq("role_id", "movie_id", "person_id")),

    JOBTable("char_name", Seq(
      int("id"), text("name"), text("imdb_index", nullable = true), int("imdb_id", nullable = true),
      text("name_pcode_nf", nullable = true), text("surname_pcode", nullable = true),
      text("md5sum", nullable = true)),
      groupFields = Seq("imdb_index")),

    JOBTable("comp_cast_type", Seq(int("id"), text("kind")), groupFields = Seq("kind")),

    JOBTable("company_name", Seq(
      int("id"), text("name"), text("country_code", nullable = true), int("imdb_id", nullable = true),
      text("name_pcode_nf", nullable = true), text("name_pcode_sf", nullable = true),
      text("md5sum", nullable = true)),
      groupFields = Seq("country_code")),

    JOBTable("company_type", Seq(int("id"), text("kind")), groupFields = Seq("kind")),

    JOBTable("complete_cast", Seq(
      int("id"), int("movie_id", nullable = true), int("subject_id"), int("status_id")),
      groupFields = Seq("subject_id", "status_id", "movie_id")),

    JOBTable("info_type", Seq(int("id"), text("info")), groupFields = Seq("info")),

    JOBTable("keyword", Seq(int("id"), text("keyword"), text("phonetic_code", nullable = true)),
      groupFields = Seq("phonetic_code")),

    JOBTable("kind_type", Seq(int("id"), text("kind")), groupFields = Seq("kind")),

    JOBTable("link_type", Seq(int("id"), text("link")), groupFields = Seq("link")),

    JOBTable("movie_companies", Seq(
      int("id"), int("movie_id"), int("company_id"), int("company_type_id"), text("note", nullable = true)),
      groupFields = Seq("company_type_id", "company_id", "movie_id")),

    JOBTable("movie_info", Seq(
      int("id"), int("movie_id"), int("info_type_id"), text("info"), text("note", nullable = true)),
      groupFields = Seq("info_type_id", "movie_id")),

    JOBTable("movie_info_idx", Seq(
      int("id"), int("movie_id"), int("info_type_id"), text("info"), text("note", nullable = true)),
      groupFields = Seq("info_type_id", "movie_id")),

    JOBTable("movie_keyword", Seq(int("id"), int("movie_id"), int("keyword_id")),
      groupFields = Seq("keyword_id", "movie_id")),

    JOBTable("movie_link", Seq(int("id"), int("movie_id"), int("linked_movie_id"), int("link_type_id")),
      groupFields = Seq("link_type_id", "movie_id")),

    JOBTable("name", Seq(
      int("id"), text("name"), text("imdb_index", nullable = true), int("imdb_id", nullable = true),
      text("gender", nullable = true), text("name_pcode_cf", nullable = true),
      text("name_pcode_nf", nullable = true), text("surname_pcode", nullable = true),
      text("md5sum", nullable = true)),
      groupFields = Seq("gender", "imdb_index")),

    JOBTable("person_info", Seq(
      int("id"), int("person_id"), int("info_type_id"), text("info"), text("note", nullable = true)),
      groupFields = Seq("info_type_id", "person_id")),

    JOBTable("role_type", Seq(int("id"), text("role")), groupFields = Seq("role")),

    JOBTable("title", Seq(
      int("id"), text("title"), text("imdb_index", nullable = true), int("kind_id"),
      int("production_year", nullable = true), int("imdb_id", nullable = true),
      text("phonetic_code", nullable = true), int("episode_of_id", nullable = true),
      int("season_nr", nullable = true), int("episode_nr", nullable = true),
      text("series_years", nullable = true), text("md5sum", nullable = true)),
      groupFields = Seq("kind_id", "production_year"))
  )

  val byName: Map[String, JOBTable] = tables.map(t => t.name -> t).toMap

  /** Lengths of the `character varying(n)` columns in JOB's schema.sql; all other text columns are `text`. */
  val varcharLength: Map[(String, String), Int] = Map(
    ("aka_name", "imdb_index") -> 12,
    ("aka_name", "name_pcode_cf") -> 5,
    ("aka_name", "name_pcode_nf") -> 5,
    ("aka_name", "surname_pcode") -> 5,
    ("aka_name", "md5sum") -> 32,
    ("aka_title", "imdb_index") -> 12,
    ("aka_title", "phonetic_code") -> 5,
    ("aka_title", "md5sum") -> 32,
    ("char_name", "imdb_index") -> 12,
    ("char_name", "name_pcode_nf") -> 5,
    ("char_name", "surname_pcode") -> 5,
    ("char_name", "md5sum") -> 32,
    ("comp_cast_type", "kind") -> 32,
    ("company_name", "country_code") -> 255,
    ("company_name", "name_pcode_nf") -> 5,
    ("company_name", "name_pcode_sf") -> 5,
    ("company_name", "md5sum") -> 32,
    ("company_type", "kind") -> 32,
    ("info_type", "info") -> 32,
    ("keyword", "phonetic_code") -> 5,
    ("kind_type", "kind") -> 15,
    ("link_type", "link") -> 32,
    ("name", "imdb_index") -> 12,
    ("name", "gender") -> 1,
    ("name", "name_pcode_cf") -> 5,
    ("name", "name_pcode_nf") -> 5,
    ("name", "surname_pcode") -> 5,
    ("name", "md5sum") -> 32,
    ("role_type", "role") -> 32,
    ("title", "imdb_index") -> 12,
    ("title", "phonetic_code") -> 5,
    ("title", "series_years") -> 49,
    ("title", "md5sum") -> 32
  )

  /**
    * `CREATE TABLE` statements for SQL Server: `INT`, and `NVARCHAR(n)` / `NVARCHAR(MAX)` for JOB's
    * `character varying(n)` / `text` (IMDB names are Unicode). Text columns use a binary collation,
    * so comparisons, LIKE and MIN are case-sensitive and ordered by code point, as in the generated
    * dataflow jobs (SQL Server's default collations are case-insensitive: '%man%' would match 'Man').
    */
  def ddlSQLServer: String = {
    val creates = tables.map { t =>
      val cols = t.columns.map { c =>
        val tp =
          if (c.isInt) "INT"
          else s"NVARCHAR(${varcharLength.get((t.name, c.name)).map(_.toString).getOrElse("MAX")}) COLLATE Latin1_General_100_BIN2"
        val constraint = if (c.name == "id") " NOT NULL PRIMARY KEY" else if (!c.nullable) " NOT NULL" else ""
        s"    ${c.name} $tp$constraint"
      }
      s"CREATE TABLE ${t.name} (\n${cols.mkString(",\n")}\n);"
    }
    s"""-- IMDB tables of the Join Order Benchmark for SQL Server, used by the generated SQL jobs.
       |-- The JOB CSV files use backslash escapes, which BULK INSERT cannot read; load them with
       |--   python sqlserver/load_sqlserver.py --dataset JOB --data <imdb_csv_dir> --connection "<ODBC connection string>"
       |
       |${creates.mkString("\n\n")}
       |""".stripMargin
  }

  /** `CREATE TABLE` statements as in JOB's schema.sql (integer / text columns, NOT NULL as there). */
  def ddl: String = {
    val creates = tables.map { t =>
      val cols = t.columns.map { c =>
        val tp = if (c.isInt) "INTEGER" else "TEXT"
        val constraint = if (c.name == "id") " NOT NULL PRIMARY KEY" else if (!c.nullable) " NOT NULL" else ""
        s"    ${c.name} $tp$constraint"
      }
      s"CREATE TABLE ${t.name} (\n${cols.mkString(",\n")}\n);"
    }
    s"""-- IMDB tables of the Join Order Benchmark, used by the generated SQL jobs.
       |-- Load the CSV files of the JOB dump (comma separated, '"' quotes, '\\' escapes), e.g. in PostgreSQL:
       |--   \\copy title FROM 'title.csv' WITH (FORMAT csv, ESCAPE '\\')
       |-- or in DuckDB (its dialect detection does not handle these files, so it is switched off):
       |--   COPY title FROM 'title.csv' (FORMAT csv, DELIMITER ',', QUOTE '"', ESCAPE '\\', HEADER false, AUTO_DETECT false);
       |
       |${creates.mkString("\n\n")}
       |""".stripMargin
  }

  def table(name: String): JOBTable =
    byName.getOrElse(name, throw new Exception(s"Can not find JOB table '$name'"))

  /**
    * Equi-join predicates that occur in the 113 JOB queries, most frequent first. Besides the
    * primary/foreign key joins, JOB joins fact tables directly on `movie_id` / `person_id`.
    *
    * The generator stores at most one join column per (column, other table) pair, so when two
    * edges compete (e.g. `title.id` with `movie_link.movie_id` and `movie_link.linked_movie_id`)
    * the more frequent one listed first wins in that direction.
    */
  val joinEdges: Seq[((String, String), (String, String))] = Seq(
    ("movie_companies", "movie_id") -> ("title", "id"),
    ("movie_keyword", "movie_id") -> ("title", "id"),
    ("keyword", "id") -> ("movie_keyword", "keyword_id"),
    ("company_name", "id") -> ("movie_companies", "company_id"),
    ("cast_info", "movie_id") -> ("title", "id"),
    ("movie_info", "movie_id") -> ("title", "id"),
    ("cast_info", "person_id") -> ("name", "id"),
    ("info_type", "id") -> ("movie_info", "info_type_id"),
    ("movie_companies", "movie_id") -> ("movie_keyword", "movie_id"),
    ("movie_info_idx", "movie_id") -> ("title", "id"),
    ("info_type", "id") -> ("movie_info_idx", "info_type_id"),
    ("movie_companies", "movie_id") -> ("movie_info", "movie_id"),
    ("company_type", "id") -> ("movie_companies", "company_type_id"),
    ("movie_info", "movie_id") -> ("movie_keyword", "movie_id"),
    ("cast_info", "movie_id") -> ("movie_keyword", "movie_id"),
    ("cast_info", "movie_id") -> ("movie_companies", "movie_id"),
    ("movie_info", "movie_id") -> ("movie_info_idx", "movie_id"),
    ("kind_type", "id") -> ("title", "kind_id"),
    ("movie_companies", "movie_id") -> ("movie_info_idx", "movie_id"),
    ("movie_info_idx", "movie_id") -> ("movie_keyword", "movie_id"),
    ("aka_name", "person_id") -> ("name", "id"),
    ("aka_name", "person_id") -> ("cast_info", "person_id"),
    ("cast_info", "person_role_id") -> ("char_name", "id"),
    ("cast_info", "movie_id") -> ("movie_info", "movie_id"),
    ("complete_cast", "movie_id") -> ("title", "id"),
    ("complete_cast", "movie_id") -> ("movie_keyword", "movie_id"),
    ("comp_cast_type", "id") -> ("complete_cast", "status_id"),
    ("cast_info", "role_id") -> ("role_type", "id"),
    ("link_type", "id") -> ("movie_link", "link_type_id"),
    ("comp_cast_type", "id") -> ("complete_cast", "subject_id"),
    ("movie_link", "movie_id") -> ("title", "id"),
    ("cast_info", "movie_id") -> ("movie_info_idx", "movie_id"),
    ("complete_cast", "movie_id") -> ("movie_info", "movie_id"),
    ("movie_companies", "movie_id") -> ("movie_link", "movie_id"),
    ("cast_info", "movie_id") -> ("complete_cast", "movie_id"),
    ("complete_cast", "movie_id") -> ("movie_companies", "movie_id"),
    ("movie_keyword", "movie_id") -> ("movie_link", "movie_id"),
    ("complete_cast", "movie_id") -> ("movie_info_idx", "movie_id"),
    ("movie_link", "linked_movie_id") -> ("title", "id"),
    ("movie_info", "movie_id") -> ("movie_link", "movie_id"),
    ("name", "id") -> ("person_info", "person_id"),
    ("cast_info", "person_id") -> ("person_info", "person_id"),
    ("info_type", "id") -> ("person_info", "info_type_id"),
    ("aka_title", "movie_id") -> ("title", "id"),
    ("aka_title", "movie_id") -> ("movie_keyword", "movie_id"),
    ("aka_title", "movie_id") -> ("movie_info", "movie_id"),
    ("aka_title", "movie_id") -> ("movie_companies", "movie_id"),
    ("complete_cast", "movie_id") -> ("movie_link", "movie_id"),
    ("movie_info_idx", "movie_id") -> ("movie_link", "movie_id"),
    ("aka_name", "person_id") -> ("person_info", "person_id")
  )

  /** How often each table occurs at most in a single JOB query (e.g. info_type AS it1, info_type AS it2). */
  val maxOccurrences: Map[String, Int] = tables.map(_.name -> 1).toMap ++ Map(
    "comp_cast_type" -> 2, "company_name" -> 2, "info_type" -> 2, "kind_type" -> 2,
    "movie_companies" -> 2, "movie_info_idx" -> 2, "title" -> 2)

  /** table -> (join column -> (other table -> other table's join column)), in both directions. */
  val joinFieldTable: Map[String, Map[String, Map[String, String]]] = {
    val directed = joinEdges.flatMap { case (a, b) => Seq(a -> b, b -> a) }
    tables.map { t =>
      val outgoing = directed.filter(_._1._1 == t.name)
      val byField = outgoing.map(_._1._2).distinct.map { f =>
        val targets = outgoing.filter(_._1._2 == f).map(_._2)
        // first (most frequent) edge wins when the same other table appears twice
        val perTable = targets.map(_._1).distinct.map(ot => ot -> targets.find(_._1 == ot).get._2)
        f -> ListMap(perTable: _*)
      }
      t.name -> ListMap(byField: _*)
    }.toMap
  }

  // sanity checks on the hand-written edge list
  joinEdges.foreach { case ((t1, c1), (t2, c2)) =>
    require(table(t1).column(c1).isInt && table(t2).column(c2).isInt, s"Join $t1.$c1 = $t2.$c2 must be on integer columns")
  }
  tables.foreach(t => require(joinFieldTable(t.name).nonEmpty, s"JOB table '${t.name}' has no join edge"))
}
