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
import org.apache.spark.sql.types._

/**
  * This example shows how to utilize the new structured streaming approach together with the
  * changes feed from couchbase.
  *
  * If no traffic runs on the couchbase bucket at start of this script, it prints
  *
  * {{{
  * +--------+-----+
  * |    type|count|
  * +--------+-----+
  * |   hotel|  917|
  * |    null|    1|
  * |landmark| 4495|
  * | airline|  187|
  * | airport| 1968|
  * |   route|24024|
  * +--------+-----+
  * }}}
  *
  * Since it keeps the counts as a total value, if you then modify a document in the UI, for
  * example a airport you'll see the airport type count increasing by one.
  *
  * @author Michael Nitschinger
  */
object StructuredStreamingExample {

  // Very simple schema, feel free to add more properties here. Properties that do not
  // exist in a streamed document show as null.
  val schema = StructType(
    StructField("META_ID", StringType) ::
    StructField("type", StringType) ::
    StructField("name", StringType) :: Nil
  )

  def main(args: Array[String]): Unit = {

    // The SparkSession is the main entry point into spark
    val spark = SparkSession
      .builder()
      .appName("N1QLExample")
      .master("local[*]") // use the JVM as the master, great for testing
      .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
      .config("spark.couchbase.bucket.travel-sample", "") // open the travel-sample bucket with empty password
      .config("com.couchbase.username", "Administrator")
      .config("com.couchbase.password", "password")
      .getOrCreate()

    // Define the Structured Stream from Couchbase with the given Schema
    val records = spark.readStream
      .format("com.couchbase.spark.sql")
      .schema(schema)
      .load()

    // Count per type and print to screen
    records
      .groupBy("type")
      .count()
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }

}
