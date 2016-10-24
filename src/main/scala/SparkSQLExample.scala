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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.EqualTo
import com.couchbase.spark.sql._

/**
  * This example shows how to work with Spark SQL / DataFrames.
  *
  * If you want to see how to use N1QL directly, please see [[N1QLExample]].
  * If you want to see Dataset conversions, check out [[DatasetExample]].
  *
  * Note that this code uses automatic schema inference based on the predicate provided when
  * creating the DataFrame. It is also possible to do manual schema inference:
  *
  * {{{
  * val airlines = sql.n1ql(StructType(
  *    StructField("name", StringType) ::
  *    StructField("abv", DoubleType) ::
  *    StructField("type", StringType) :: Nil
  * ))
  * }}}
  *
  * @author Michael Nitschinger
  */
object SparkSQLExample {

  def main(args: Array[String]): Unit = {

    // The SparkSession is the main entry point into spark
    val spark = SparkSession
      .builder()
      .appName("SparkSQLExample")
      .master("local[*]") // use the JVM as the master, great for testing
      .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
      .config("spark.couchbase.bucket.travel-sample", "") // open the travel-sample bucket with empty password
      .getOrCreate()

    // Creates DataFrames with automatic schema inference based on a "type" predicate
    val airlines = spark.read.couchbase(EqualTo("type", "airline"))
    val airports = spark.read.couchbase(EqualTo("type", "airport"))

    // The inferred schemata can be nicely printed to cross-check
    println(s"Airline Schema: ${airlines.schema.treeString}")
    println(s"Airport Schema: ${airports.schema.treeString}")

    // Querying the Airlines DataFrame through Spark SQL
    println("Name and callsign for 10 airlines:")
    airlines
      .select("name", "callsign")
      .sort(airlines("callsign").desc)
      .show(10)

    // Counting all Airports
    println(s"Number of Airports: ${airports.count()}")

    // Group and count airports by country
    println("Airports by Country:")
    airports
      .groupBy("country")
      .count()
      .show()
  }

}
