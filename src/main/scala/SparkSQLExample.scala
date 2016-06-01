/*
 * Copyright (c) 2015 Couchbase, Inc.
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
import com.couchbase.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.{SparkConf, SparkContext}
import com.couchbase.spark.sql._

object SparkSQLExample {

  def main(args: Array[String]): Unit = {

    // Configure Spark
    val cfg = new SparkConf()
      .setAppName("n1qlQueryExample")
      .setMaster("local[*]")
      .set("com.couchbase.bucket.travel-sample", "")

    // Generate The Context
    val sc = new SparkContext(cfg)

    // Spark SQL Setup
    val sql = new SQLContext(sc)

    // Plain old Spark emitting RDDs from a N1QL Query result
    sc.couchbaseQuery(N1qlQuery.simple("SELECT * from `travel-sample` LIMIT 10"))
      .collect()
      .foreach(println)

    // Create a DataFrame with Schema Inference
    val df = sql.read.couchbase(schemaFilter = EqualTo("type", "airline"))

    // A DataFrame can also be created from an explicit schema
    /*val df = sql.n1ql(StructType(
      StructField("name", StringType) ::
      StructField("abv", DoubleType) ::
      StructField("type", StringType) :: Nil
    ))*/

    // Print The Schema
    df.printSchema()

    // SparkSQL Integration
    df
      .select("name", "callsign")
      .sort(df("callsign").desc)
      .show(10)
  }



}
