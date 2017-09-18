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

/** Airline has subset of the fields that are in the database */
case class Airline(name: String, iata: String, icao: String, country: String)

/**
  * This example is very similar to [[SparkSQLExample]] but it shows how a DataFrame is converted into a
  * Dataset and can then be used through regular scala code instead of SparkSQL syntax while still preserving
  * all the optimizations of the underlying spark execution engine.
  *
  * @author Michael Nitschinger
  */
object DatasetExample {

  def main(args: Array[String]): Unit = {

    // The SparkSession is the main entry point into spark
    val spark = SparkSession
      .builder()
      .appName("DatasetExample")
      .master("local[*]") // use the JVM as the master, great for testing
      .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
      .config("spark.couchbase.bucket.travel-sample", "") // open the travel-sample bucket with empty password
      .config("com.couchbase.username", "Administrator")
      .config("com.couchbase.password", "password")
      .getOrCreate()

    // Import needed for case class conversions
    import spark.implicits._

    // Create the DataFrame and convert it into a Dataset through `as[Airline]`
    val airlines = spark.read.couchbase(EqualTo("type", "airline")).as[Airline]

    // Print schema like on a DataFrame
    airlines.printSchema()

    // Print airlines that start with A, using a "scala" API instead of SparkSQL syntax.
    airlines
      .map(_.name)
      .filter(_.toLowerCase.startsWith("a"))
      .foreach(println(_))
  }

}
