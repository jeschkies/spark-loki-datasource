import sbt._

object Dependencies {
  // Versions
  lazy val sparkVersion = "3.1.2"

  // Libraries
  val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion 
  val sparkSql= "org.apache.spark" %% "spark-sql" % sparkVersion 

  // Projects
  val backendDeps =
    Seq(sparkStreaming, sparkSql)
}
