name := "couchbase-spark-samples"

organization := "com.couchbase"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-streaming" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "com.couchbase.client" %% "spark-connector" % "1.2.1",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1",
  "org.apache.spark" %% "spark-mllib" % "1.6.1",
    "mysql" % "mysql-connector-java" % "5.1.37"
)