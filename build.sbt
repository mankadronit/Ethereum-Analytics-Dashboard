scalaVersion := "2.12.15"

name := "EthereumAnalytics"
version := "1.0"

val sparkVersion = "3.2.1"
val kafkaVersion = "3.1.0"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.11" % "test",
  "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided",
  "org.apache.kafka" %% "kafka" % "3.1.0",
  "org.apache.kafka" % "kafka-clients" % "3.1.0",
  "org.apache.kafka" % "kafka-streams" % "3.1.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.1" ,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1" % Test,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.2.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.13.2",
  "mysql" % "mysql-connector-java" % "8.0.28",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13"
)

ThisBuild / assemblyMergeStrategy  := {
  case PathList(ps @ _*) if ps.last endsWith "module-info.class" => MergeStrategy.first
  case x if x.contains("io.netty.versions.properties") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "UnusedStubClass.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Logger.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "nowarn$.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "nowarn.class" => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

