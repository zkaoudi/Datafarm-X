package instantiation.dataset.tpch

import instantiation.platform.sql.AbstractSQLTableOperatorManager
import instantiation.dataset.AbstractTableOperatorManager


/** TPC-H column names as created by dbgen's `dss.ddl`: table prefix + lower-case field, e.g. `l_orderkey`. */
trait TPCHSqlNaming extends AbstractSQLTableOperatorManager {
  override def sqlColumn(field: String): String = TPCHSql.prefix(tableName) + field.toLowerCase
}

object TPCHSql {

  val prefix: Map[String, String] = Map(
    "lineitem" -> "l_", "orders" -> "o_", "customer" -> "c_", "part" -> "p_",
    "partsupp" -> "ps_", "supplier" -> "s_", "nation" -> "n_", "region" -> "r_")

  private def sqlType(scalaType: String, table: String): String = scalaType match {
    case "Int" => "INTEGER"
    case "Float" => "DECIMAL(15,2)"
    case "String" => "VARCHAR(199)"
    case "java.sql.Date" => "DATE"
    case other => throw new Exception(s"Unsupported TPC-H field type '$other' in table '$table'")
  }

  /** Column types of dbgen's dss.ddl. */
  val officialType: Map[String, String] = Map(
    "n_nationkey" -> "INTEGER",
    "n_name" -> "CHAR(25)",
    "n_regionkey" -> "INTEGER",
    "n_comment" -> "VARCHAR(152)",
    "r_regionkey" -> "INTEGER",
    "r_name" -> "CHAR(25)",
    "r_comment" -> "VARCHAR(152)",
    "p_partkey" -> "INTEGER",
    "p_name" -> "VARCHAR(55)",
    "p_mfgr" -> "CHAR(25)",
    "p_brand" -> "CHAR(10)",
    "p_type" -> "VARCHAR(25)",
    "p_size" -> "INTEGER",
    "p_container" -> "CHAR(10)",
    "p_retailprice" -> "DECIMAL(15,2)",
    "p_comment" -> "VARCHAR(23)",
    "s_suppkey" -> "INTEGER",
    "s_name" -> "CHAR(25)",
    "s_address" -> "VARCHAR(40)",
    "s_nationkey" -> "INTEGER",
    "s_phone" -> "CHAR(15)",
    "s_acctbal" -> "DECIMAL(15,2)",
    "s_comment" -> "VARCHAR(101)",
    "ps_partkey" -> "INTEGER",
    "ps_suppkey" -> "INTEGER",
    "ps_availqty" -> "INTEGER",
    "ps_supplycost" -> "DECIMAL(15,2)",
    "ps_comment" -> "VARCHAR(199)",
    "c_custkey" -> "INTEGER",
    "c_name" -> "VARCHAR(25)",
    "c_address" -> "VARCHAR(40)",
    "c_nationkey" -> "INTEGER",
    "c_phone" -> "CHAR(15)",
    "c_acctbal" -> "DECIMAL(15,2)",
    "c_mktsegment" -> "CHAR(10)",
    "c_comment" -> "VARCHAR(117)",
    "o_orderkey" -> "INTEGER",
    "o_custkey" -> "INTEGER",
    "o_orderstatus" -> "CHAR(1)",
    "o_totalprice" -> "DECIMAL(15,2)",
    "o_orderdate" -> "DATE",
    "o_orderpriority" -> "CHAR(15)",
    "o_clerk" -> "CHAR(15)",
    "o_shippriority" -> "INTEGER",
    "o_comment" -> "VARCHAR(79)",
    "l_orderkey" -> "INTEGER",
    "l_partkey" -> "INTEGER",
    "l_suppkey" -> "INTEGER",
    "l_linenumber" -> "INTEGER",
    "l_quantity" -> "DECIMAL(15,2)",
    "l_extendedprice" -> "DECIMAL(15,2)",
    "l_discount" -> "DECIMAL(15,2)",
    "l_tax" -> "DECIMAL(15,2)",
    "l_returnflag" -> "CHAR(1)",
    "l_linestatus" -> "CHAR(1)",
    "l_shipdate" -> "DATE",
    "l_commitdate" -> "DATE",
    "l_receiptdate" -> "DATE",
    "l_shipinstruct" -> "CHAR(25)",
    "l_shipmode" -> "CHAR(10)",
    "l_comment" -> "VARCHAR(44)"
  )

  /** Primary keys of dbgen's dss.ri. */
  val primaryKey: Map[String, Seq[String]] = Map(
    "region" -> Seq("r_regionkey"),
    "nation" -> Seq("n_nationkey"),
    "part" -> Seq("p_partkey"),
    "supplier" -> Seq("s_suppkey"),
    "partsupp" -> Seq("ps_partkey", "ps_suppkey"),
    "customer" -> Seq("c_custkey"),
    "lineitem" -> Seq("l_orderkey", "l_linenumber"),
    "orders" -> Seq("o_orderkey")
  )

  /**
    * `CREATE TABLE` statements for SQL Server with the types of dbgen's dss.ddl and the primary keys
    * of dss.ri, which SQL Server's optimizer uses (so its plans resemble those of a standard TPC-H setup).
    */
  def ddlSQLServer(tables: Seq[AbstractTableOperatorManager]): String = {
    val creates = tables.map { t =>
      val names = t.fields.toSeq.sortBy(_._2.toInt).map(f => t.sqlColumn(f._1))
      val cols = names.map(c => s"    $c ${officialType.getOrElse(c, throw new Exception(s"No dss.ddl type for TPC-H column '$c'"))} NOT NULL")
      val pk = primaryKey.get(t.tableName).map(k => s",\n    PRIMARY KEY (${k.mkString(", ")})").getOrElse("")
      s"CREATE TABLE ${t.tableName} (\n${cols.mkString(",\n")}$pk\n);"
    }
    s"""-- TPC-H tables for SQL Server (types of dbgen's dss.ddl, primary keys of dss.ri), used by the generated SQL jobs.
       |-- Load the .tbl files with BULK INSERT (paths as seen by the SQL Server process), e.g.
       |--   BULK INSERT lineitem FROM '/data/lineitem.tbl' WITH (FIELDTERMINATOR = '|', ROWTERMINATOR = '|\\n', TABLOCK);
       |-- or from any client with
       |--   python platforms/sql/load_sqlserver.py --dataset TPCH --data <tbl_dir> --connection "<ODBC connection string>"
       |
       |${creates.mkString("\n\n")}
       |""".stripMargin
  }

  /** `CREATE TABLE` statements for the given TPC-H tables, columns in file order. */
  def ddl(tables: Seq[AbstractTableOperatorManager]): String = {
    val creates = tables.map { t =>
      val types = t.typeSchema.trim.stripPrefix("(").stripSuffix(")").split(",").map(_.trim)
      val names = t.fields.toSeq.sortBy(_._2.toInt).map(_._1)
      require(types.length == names.size, s"typeSchema and fields of '${t.tableName}' differ in length")
      val cols = names.zip(types).map { case (n, tp) => s"    ${t.sqlColumn(n)} ${sqlType(tp, t.tableName)} NOT NULL" }
      s"CREATE TABLE ${t.tableName} (\n${cols.mkString(",\n")}\n);"
    }
    s"""-- TPC-H tables used by the generated SQL jobs (column names as in dbgen's dss.ddl).
       |-- Load the .tbl files, e.g. in PostgreSQL after removing the trailing '|' of each line:
       |--   \\copy lineitem FROM 'lineitem.tbl' WITH (FORMAT csv, DELIMITER '|')
       |-- or in DuckDB:
       |--   COPY lineitem FROM 'lineitem.tbl' (DELIMITER '|', HEADER false, NULL_PADDING true);
       |
       |${creates.mkString("\n\n")}
       |""".stripMargin
  }
}
