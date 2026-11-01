package Generator.DatasetJob

/** Mixed into a SQL table manager to generate T-SQL for SQL Server. */
trait SQLServerSQL extends AbstractSQLTableOperatorManager {
  override def sqlDialect: SQLDialect = SQLDialect.SQLServer
}
