package instantiation.dataset

/**
  * The reduce function of generated jobs: keep the smaller of two records under a total order.
  *
  * Group-bys and reduces in generated jobs keep one whole record per group (so later operators see
  * the table's schema). Spark, Wayang and Flink apply reduce functions to a group's records in any
  * order and grouping (within partitions, then across them), so the function must be associative
  * and commutative, or the kept record, and everything computed from it, depends on the engine and
  * the data layout. "Keep the first" (`(x1, x2) => x1`) is not commutative; the minimum under a
  * total order is.
  *
  * Records are compared field by field (lexicographically), values by their natural order with
  * nulls first, so the kept record is the same on every engine and every run. The SQL jobs keep the
  * same record with `ROW_NUMBER() OVER (... ORDER BY <all columns>)` (see `AbstractSQLTableOperatorManager`).
  *
  * [[code]] is the Scala source of the helper functions, generated into each job: into the parser
  * object of Spark and Wayang jobs, and into the job object of Flink jobs.
  */
object RecordOrder {

  /** Name of the reduce function in the generated code. */
  val Function = "minRecord"

  /** A reduce UDF keeping the smaller record; `qualifier` is the object holding [[code]], or "" in Flink jobs. */
  def udf(qualifier: String = ""): String = {
    val f = if (qualifier.isEmpty) Function else s"$qualifier.$Function"
    s"(r1, r2) => $f(r1, r2)"
  }

  /** Scala source of minRecord and its comparisons, indented for a member of an object. */
  val code: String =
    """  /**
      |   * The smaller of two records, comparing them field by field (nulls first). Associative and
      |   * commutative, so as a reduce function it keeps the same record on every engine and every run.
      |   */
      |  def minRecord[T <: Product](a: T, b: T): T = if (compareRecords(a, b) <= 0) a else b
      |
      |  def compareRecords(a: Product, b: Product): Int = {
      |    val n = math.min(a.productArity, b.productArity)
      |    var i = 0
      |    var c = 0
      |    while (c == 0 && i < n) {
      |      c = compareValues(a.productElement(i), b.productElement(i))
      |      i += 1
      |    }
      |    if (c != 0) c else Integer.compare(a.productArity, b.productArity)
      |  }
      |
      |  def compareValues(x: Any, y: Any): Int = (x, y) match {
      |    case (null, null) => 0
      |    case (null, _) => -1
      |    case (_, null) => 1
      |    case (p: Int, q: Int) => Integer.compare(p, q)
      |    case (p: Long, q: Long) => java.lang.Long.compare(p, q)
      |    case (p: Float, q: Float) => java.lang.Float.compare(p, q)
      |    case (p: Double, q: Double) => java.lang.Double.compare(p, q)
      |    case (p: String, q: String) => p.compareTo(q)
      |    case (p: Product, q: Product) => compareRecords(p, q)
      |    case (p: Comparable[_], _) => p.asInstanceOf[Comparable[Any]].compareTo(y)
      |    case _ => x.toString.compareTo(y.toString)
      |  }""".stripMargin
}
