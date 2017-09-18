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
import com.couchbase.client.java.document.JsonDocument
import org.apache.spark.sql.SparkSession
import com.couchbase.spark._

/**
  * This example fetches two documents by its document ID and prints out their contents.
  *
  * This prints:
  *
  * {{{
  * JsonDocument{id='airline_10748', cas=312610725036034, expiry=0,
  * content={"country":"United States","iata":"ZQ","name":"Locair", "callsign":"LOCAIR","icao":"LOC","id":10748,"type":"airline"},
  * mutationToken=null}
  *
  * JsonDocument{id='airline_10123', cas=312605445586946, expiry=0,
  * content={"country":"United States","iata":"TQ","name":"Texas Wings","callsign":"TXW","icao":"TXW","id":10123,"type":"airline"},
  * mutationToken=null}
  * }}}
  *
  * @author Michael Nitschinger
  */
object KeyValueExample {

  def main(args: Array[String]): Unit = {

    // The SparkSession is the main entry point into spark
    val spark = SparkSession
      .builder()
      .appName("KeyValueExample")
      .master("local[*]") // use the JVM as the master, great for testing
      .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
      .config("spark.couchbase.bucket.travel-sample", "") // open the travel-sample bucket with empty password
      .config("com.couchbase.username", "Administrator")
      .config("com.couchbase.password", "password")
      .getOrCreate()

    spark.sparkContext
      .couchbaseGet[JsonDocument](Seq("airline_10123", "airline_10748")) // Load documents from couchbase
      .collect() // collect all data from the spark workers
      .foreach(println) // print each document content
  }

}
