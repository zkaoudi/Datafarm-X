package instantiation.platform.sql

/**
  * The few constructs in which the generated SQL differs between engines.
  *
  * [[SQLDialect.Standard]] is standard SQL (PostgreSQL, DuckDB, ...); [[SQLDialect.SQLServer]] is
  * T-SQL, which has no `DATE '...'` literal, rejects `ORDER BY` in a common table expression unless
  * it has `TOP`/`OFFSET` (error 1033), and treats `[` in `LIKE` patterns as the start of a
  * character class.
  */
sealed trait SQLDialect {
  def name: String

  /** A date constant, from `yyyy-MM-dd`. */
  def dateLiteral(date: String): String

  /** A timestamp constant, from `yyyy-MM-dd HH:mm:ss.SSS`. */
  def timestampLiteral(timestamp: String): String

  /** The ORDER BY clause of a sort operator inside a common table expression. */
  def orderByInCte(column: String): String

  /** A LIKE pattern (SQL wildcards `%` and `_`) as a string literal body, without quotes. */
  def likePattern(pattern: String): String
  /** An ascending ORDER BY item with NULLs first (the order of the dataflow jobs' minRecord). */
  def ascNullsFirst(column: String): String
}

object SQLDialect {

  object Standard extends SQLDialect {
    val name = "standard SQL"

    def dateLiteral(date: String): String = s"DATE '$date'"

    def timestampLiteral(timestamp: String): String = s"TIMESTAMP '$timestamp'"

    def orderByInCte(column: String): String = s"ORDER BY $column"

    def likePattern(pattern: String): String = pattern
    def ascNullsFirst(column: String): String = s"$column NULLS FIRST"
  }

  object SQLServer extends SQLDialect {
    val name = "SQL Server (T-SQL)"

    def dateLiteral(date: String): String = s"CAST('$date' AS DATE)"

    def timestampLiteral(timestamp: String): String = s"CAST('$timestamp' AS DATETIME2(3))"

    // OFFSET 0 ROWS makes ORDER BY legal in a CTE; as in standard SQL, the order is only guaranteed for the final result
    def orderByInCte(column: String): String = s"ORDER BY $column OFFSET 0 ROWS"

    // '[' would start a character class; [[] matches a literal '['
    def likePattern(pattern: String): String = pattern.replace("[", "[[]")
    // T-SQL sorts NULLs first in ascending order and has no NULLS FIRST
    def ascNullsFirst(column: String): String = column
  }
}
