name := "couchbase-spark-samples"

organization := "com.couchbase"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.12.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "com.couchbase.client" %% "spark-connector" % "2.4.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.0",
  "mysql" % "mysql-connector-java" % "5.1.37"
)

resolvers += Resolver.mavenLocal