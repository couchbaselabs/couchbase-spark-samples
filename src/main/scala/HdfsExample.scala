/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.couchbase.client.java.query.N1qlQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.EqualTo
import com.couchbase.spark.sql._
import com.couchbase.spark._

/**
  * This example shows how to combine DataFrames from Couchbase and HDFS together to perform useful queries.
  *
  * Note that when you run it for the first time run this script as-is, but then comment out the first block
  * where the records are initially imported into HDFS, this is just the "data loading" phase for this
  * example.
  *
  * Make sure HDFS runs on port 9000 on localhost or adapt the read/write paths in this example.
  *
  * @author Michael Nitschinger
  * @author Matt Ingenthron
  */
object HdfsExample {

  def main(args: Array[String]): Unit = {

    // The SparkSession is the main entry point into spark
    val spark = SparkSession
      .builder()
      .appName("HdfsExample")
      .master("local[*]") // use the JVM as the master, great for testing
      .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
      .config("spark.couchbase.bucket.travel-sample", "") // open the travel-sample bucket with empty password
      .getOrCreate()

    // ---- Write Data into HDFS  (! do once to load and then comment out ... !) ----
    val query = "SELECT `travel-sample`.* from `travel-sample` WHERE type = 'landmark'"
    spark.sparkContext
      .couchbaseQuery(N1qlQuery.simple(query))
      .map(_.value.toString)
      .saveAsTextFile("hdfs://127.0.0.1:9000/landmarks")

    // ---- Load data from HDFS and join with records in Couchbase ----

    // Load Landmarks from HDFS
    val landmarks = spark.read.json("hdfs://127.0.0.1:9000/landmarks/*")
    landmarks.createOrReplaceTempView("landmarks")

    // Load Airports from Couchbase
    val airports = spark.read.couchbase(schemaFilter = EqualTo("type", "airport"))

    // find all landmarks in the same city as the given FAA code
    val toFind = "SFO" // try SFO or LAX
    airports
      .join(landmarks, airports("city") === landmarks("city"))
      .where(airports("faa") === toFind and landmarks("url").isNotNull)
      .select(landmarks("name"), landmarks("address"), airports("faa"))
      .orderBy(landmarks("name").asc)
      .show()
  }
}
