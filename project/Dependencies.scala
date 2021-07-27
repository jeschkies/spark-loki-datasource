import sbt._

object Dependencies {
  // Versions
  lazy val sparkVersion = "3.1.2"

  // Libraries
  val scalaj = "org.scalaj" %% "scalaj-http" % "2.4.2"
  val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
  val sparkSql= "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" 

  val ujson = "com.lihaoyi" %% "ujson" % "0.9.6"

  // Projects
  val backendDeps =
    Seq(scalaj, sparkStreaming, sparkSql, ujson)
}
