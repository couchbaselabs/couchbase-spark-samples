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
import com.couchbase.spark._
import org.apache.spark.sql.SparkSession

/**
  * This example shows how to use the Subdocument API which has been introduced with
  * Couchbase Server 4.5.0.
  *
  * @author Michael Nitschinger
  */
object SubdocLookupExample {

  def main(args: Array[String]): Unit = {

    // The SparkSession is the main entry point into spark
    val spark = SparkSession
      .builder()
      .appName("SubdocExample")
      .master("local[*]") // use the JVM as the master, great for testing
      .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
      .config("spark.couchbase.bucket.travel-sample", "") // open the travel-sample bucket with empty password
      .config("com.couchbase.username", "Administrator")
      .config("com.couchbase.password", "password")
      .getOrCreate()


    val result = spark.sparkContext.couchbaseSubdocLookup(
      Seq("airline_10123"), // fetch these document ids
      Seq("name", "iata"),  // only fetch their name and iata code
      Seq("foobar") // but also check if the foobar key exists in the doc
    ).collect()

    // Prints
    // SubdocLookupResult(
    //    airline_10123,0,Map(name -> Texas Wings, iata -> TQ),Map(foobar -> false)
    // )
    result.foreach(println)

    // Same as above, but omits the exists check, just looks up the fields.
    val r2  = spark.sparkContext.couchbaseSubdocLookup(
      Seq("airline_10123"),
      Seq("name", "iata")
    )

    // Prints
    // SubdocLookupResult(
    //    airline_10123,0,Map(name -> Texas Wings, iata -> TQ),Map()
    // )
    r2.foreach(println)

  }
}