package instantiation.dataset.tpch

import instantiation.dataset.AbstractDatasetOperatorManager
import instantiation.AbstractJob
import instantiation.platform.sql.AbstractSQLJob
import instantiation.platform.sql.AbstractSQLServerFlatJob
import instantiation.platform.spark.AbstractSparkJob
import instantiation.platform.spark.AbstractSparkTableOperatorManager
import instantiation.dataset.AbstractTableOperatorManager
import instantiation.platform.wayang.AbstractWayangTableOperatorManager
import instantiation.dataset.JoinRelation
import instantiation.platform.sql.SQLServerSQL


import scala.collection.mutable
import scala.util.Random

/**
  * Created by researchuser7 on 2020-04-20.
  */


class TPCHDatasetOperatorManager extends AbstractDatasetOperatorManager {

  val tables: Seq[String] = Seq(
    "lineitem",
    "orders",
    "customer",
    "partsupp",
    "part",
    "supplier"//,
    //"nation"
  )
  def this(platform: String) {
    this()
    this.platform = platform
  }
  def getTableOperatorManager(s: String, p: String): AbstractTableOperatorManager  = 
  p match {
    case "Wayang" => {
       s match {
          case "lineitem" => new LineitemOperatorManager with AbstractWayangTableOperatorManager 
          case "orders" => new OrdersOperatorManager with AbstractWayangTableOperatorManager
          case "customer" => new CustomerOperatorManager with AbstractWayangTableOperatorManager
          case "partsupp" => new PartsuppOperatorManager with AbstractWayangTableOperatorManager
          case "part" => new PartOperatorManager with AbstractWayangTableOperatorManager
          case "supplier" => new SupplierOperatorManager with AbstractWayangTableOperatorManager
          case "nation" => new NationOperatorManager with AbstractWayangTableOperatorManager
          case _ => throw new Exception(s"Can not find table operator manager for table '$s' ")
       }
    }
    case "Spark" => {
       s match {
          case "lineitem" => new LineitemOperatorManager with AbstractSparkTableOperatorManager
          case "orders" => new OrdersOperatorManager with AbstractSparkTableOperatorManager
          case "customer" => new CustomerOperatorManager with AbstractSparkTableOperatorManager
          case "partsupp" => new PartsuppOperatorManager with AbstractSparkTableOperatorManager
          case "part" => new PartOperatorManager with AbstractSparkTableOperatorManager
          case "supplier" => new SupplierOperatorManager with AbstractSparkTableOperatorManager
          case "nation" => new NationOperatorManager with AbstractSparkTableOperatorManager
          case _ => throw new Exception(s"Can not find table operator manager for table '$s' ")
       }
    }
    case "SQL" => {
       s match {
          case "lineitem" => new LineitemOperatorManager with TPCHSqlNaming
          case "orders" => new OrdersOperatorManager with TPCHSqlNaming
          case "customer" => new CustomerOperatorManager with TPCHSqlNaming
          case "partsupp" => new PartsuppOperatorManager with TPCHSqlNaming
          case "part" => new PartOperatorManager with TPCHSqlNaming
          case "supplier" => new SupplierOperatorManager with TPCHSqlNaming
          case "nation" => new NationOperatorManager with TPCHSqlNaming
          case _ => throw new Exception(s"Can not find table operator manager for table '$s' ")
       }
    }
    case "SQLServer" => {
       s match {
          case "lineitem" => new LineitemOperatorManager with TPCHSqlNaming with SQLServerSQL
          case "orders" => new OrdersOperatorManager with TPCHSqlNaming with SQLServerSQL
          case "customer" => new CustomerOperatorManager with TPCHSqlNaming with SQLServerSQL
          case "partsupp" => new PartsuppOperatorManager with TPCHSqlNaming with SQLServerSQL
          case "part" => new PartOperatorManager with TPCHSqlNaming with SQLServerSQL
          case "supplier" => new SupplierOperatorManager with TPCHSqlNaming with SQLServerSQL
          case "nation" => new NationOperatorManager with TPCHSqlNaming with SQLServerSQL
          case _ => throw new Exception(s"Can not find table operator manager for table '$s' ")
       }
    }
    case "SQLServerFlat" => {
       s match {
          case "lineitem" => new LineitemOperatorManager with TPCHSQLServerFlat
          case "orders" => new OrdersOperatorManager with TPCHSQLServerFlat
          case "customer" => new CustomerOperatorManager with TPCHSQLServerFlat
          case "partsupp" => new PartsuppOperatorManager with TPCHSQLServerFlat
          case "part" => new PartOperatorManager with TPCHSQLServerFlat
          case "supplier" => new SupplierOperatorManager with TPCHSQLServerFlat
          case "nation" => new NationOperatorManager with TPCHSQLServerFlat
          case _ => throw new Exception(s"Can not find table operator manager for table '$s' ")
       }
    }
    case _ => { 
      s match {
          case "lineitem" => LineitemOperatorManager()
          case "orders" => OrdersOperatorManager()
          case "customer" => CustomerOperatorManager()
          case "partsupp" => PartsuppOperatorManager()
          case "part" => PartOperatorManager()
          case "supplier" => SupplierOperatorManager()
          case "nation" => NationOperatorManager()
          case _ => throw new Exception(s"Can not find table operator manager for table '$s' ")
        }
      }
    }

  override def sparkJob(jobBody: String, jobName: String, tables: Seq[AbstractTableOperatorManager]): AbstractJob =
    AbstractSparkJob(jobBody, jobName, obj => TPCHParsers.code(obj, tables), "TPC-H")

  /**
    * For SQLServerFlat every table may occur only once (the flat query format has no aliases), so the
    * join sequence is drawn again from other seeds until its tables are distinct.
    */
  override def getSourceTableSequence(nJoins: Int, seed: Int = 0): (Seq[AbstractTableOperatorManager], Seq[JoinRelation]) =
    if (platform != "SQLServerFlat") super.getSourceTableSequence(nJoins, seed)
    else (0 until 1000).iterator
      .map(i => scala.util.Try(super.getSourceTableSequence(nJoins, seed + 1000 * i)).toOption)
      .collectFirst { case Some((ts, js)) if ts.map(_.tableName).distinct.size == ts.size => (ts, js) }
      .getOrElse(throw new Exception(s"No sequence of $nJoins joins over distinct TPC-H tables (required by SQLServerFlat)"))

  override def sqlJob(jobBody: String, jobName: String, tables: Seq[AbstractTableOperatorManager]): AbstractJob = {
    if (platform == "SQLServerFlat") return AbstractSQLServerFlatJob(jobBody, jobName)
    val all = (this.tables :+ "nation").map(t => getTableOperatorManager(t, "SQL"))
    if (platform == "SQLServer") AbstractSQLJob(jobBody, jobName, "TPC-H (SQL Server)", TPCHSql.ddlSQLServer(all))
    else AbstractSQLJob(jobBody, jobName, "TPC-H", TPCHSql.ddl(all))
  }

  def main(args: Array[String]): Unit = {
    for (i <- 0 to 100){
      try{
        val (tableSequence, joinSequence) = getSourceTableSequence(5, i)
        joinSequence.foreach( j => println(s"> $j"))
        println()
        println(s"Table sequence: $tableSequence")
      } catch {
        case ex:Exception => println(ex)
      } finally {
        println("---------------------------------------------")
      }
    }
  }
}
