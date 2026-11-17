package instantiation.dataset

/**
  * Generated jobs that parse their input themselves (TPC-H and JOB on Wayang and Spark) put the
  * parse functions into an object `<jobName>Parsers` in the same file. Operator code refers to it
  * through this placeholder, which the job template replaces with the actual object name.
  */
object ParserCode {
  val Token = "__PARSERS__"
}
