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
import com.couchbase.spark._

/**
  * This example shows how to perform a "raw" N1QL query generating an RDD.
  *
  * If you want to work with Spark SQL, please see [[SparkSQLExample]].
  *
  * This code prints:
  *
  * {{{
  * {"count":1560,"country":"United States"}
  * {"count":221,"country":"France"}
  * {"count":187,"country":"United Kingdom"}
  * }}}
  *
  * @author Michael Nitschinger
  */
object N1QLExample {

  def main(args: Array[String]): Unit = {

    // The SparkSession is the main entry point into spark
    val spark = SparkSession
      .builder()
      .appName("N1QLExample")
      .master("local[*]") // use the JVM as the master, great for testing
      .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
      .config("spark.couchbase.bucket.travel-sample", "") // open the travel-sample bucket with empty password
      .getOrCreate()

    // This query groups airports by country and counts them.
    val query = N1qlQuery.simple("" +
      "select country, count(*) as count " +
      "from `travel-sample` " +
      "where type = 'airport' " +
      "group by country " +
      "order by count desc")

    // Perform the query and print the country name and the count.
    spark.sparkContext
      .couchbaseQuery(query)
      .map(_.value)
      .foreach(println)
  }
}
