name := "couchbase-spark-samples"

organization := "com.couchbase"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0",
  "org.apache.spark" %% "spark-streaming" % "1.3.0",
  "org.apache.spark" %% "spark-sql" % "1.3.0",
  "com.couchbase.client" %% "spark-connector" % "1.0.0-SNAPSHOT"
)

resolvers += Resolver.mavenLocal