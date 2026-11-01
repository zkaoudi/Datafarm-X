package instantiation

trait AbstractJob {
  var filledBody:String = fillBody()
  var filledSBT:String = fillSBT()

  def fillBody(): String
  
  def fillSBT(): String

  def createProject(projectPath:String): Unit 
}


object AbstractJob {

  /**
    * sbt version of the generated sbt projects (Flink, Spark), the one this generator builds with. Without it, a
    * recent sbt launcher picks sbt 2, which needs JDK 17 and does not load the sbt 1 plugins of the Flink projects.
    */
  val SbtVersion = "1.9.9"

  /** Writes `<projectPath>/project/build.properties` pinning [[SbtVersion]]. */
  def writeBuildProperties(projectPath: String): Unit = {
    val dir = new java.io.File(projectPath, "project")
    dir.mkdirs()
    val w = new java.io.PrintWriter(new java.io.File(dir, "build.properties"), "UTF-8")
    try w.write(s"sbt.version=$SbtVersion\n") finally w.close()
  }
}
