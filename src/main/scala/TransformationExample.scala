/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.spark._
import com.couchbase.spark.streaming._
import org.apache.spark.sql.{DataFrameReader, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/** A sample Apache Spark program to show how Couchbase may be used with Spark
  * when doing data transformations.
  *
  * Assuming a MySQL Database and documents with this format:
  *
  * {
  *  "givenname": "Matt",
  *   "surname": "Ingenthron",
  *   "email": "matt@email.com"
  * }
  *
  * Stream out all documents, look them up in the data loaded from mysql, join on
  * the email address and add the entitlement token.
  */
object TransformationExample {

  val conf = new SparkConf().setMaster("local[*]")
    .setAppName("TransformationExample")
    .set("com.couchbase.bucket.transformative", "letmein") // Configure for the Couchbase bucket "transformative" with "password"

  val sc = new SparkContext(conf)

  /** Returns a JsonDocument based on a tuple of two strings */
  def CreateDocument(s: (String, String)): JsonDocument = {
    JsonDocument.create(s._1, JsonObject.fromJson(s._2))
  }

  /** Returns an RDD based on email address extraced from the document */
  def CreateMappableRdd(s: (String, String)): (String, JsonDocument) = {
    val return_doc = JsonDocument.create(s._1, JsonObject.fromJson(s._2))
    (return_doc.content().getString("email"), return_doc)
  }

  /** Returns a JsonDocument enriched with the entitlement token */
  def mergeIntoDoc(t: (String, (JsonDocument, Integer))): JsonDocument = {
    val jsonToEnrich = t._2._1.content()
    val entitlementFromJoin = t._2._2
    jsonToEnrich.put("entitlementtoken", entitlementFromJoin)
    t._2._1
  }

  def getMysqlReader(sqlctx: SQLContext): DataFrameReader = {

    // Now get set up to fetch things from MySQL
    // The database name is ext_users with the data we want in a table named profiles
    // and a read-only user named profiles
    val mysql_connstr = "jdbc:mysql://localhost:3306/ext_users"
    val mysql_uname = "profiles"
    val mysql_password = "profiles"

    sqlctx.read.format("jdbc").options(
      Map("url" -> (mysql_connstr + "?user=" + mysql_uname + "&password=" + mysql_password),
        "dbtable" -> "ext_users.profiles"))
  }

  def main(args: Array[String]): Unit = {

    System.setProperty("com.couchbase.dcpEnabled", "true")

    Class.forName("com.mysql.jdbc.Driver").newInstance // Load the MySQL Connector


    val mysqlReader = getMysqlReader(new org.apache.spark.sql.SQLContext(sc)) // set up a MySQL Reader

    // Note that if the database was quite large you could push down other predicates to MySQL or partition
    // the DataFrame
    //    mysqlReader.load().filter("email = \"matt@email.com\"")



    // load the DataFrame of all of the users from MySQL.
    // Note, appending .cache() may make sense here (or not) depending on amount of data.
    val entitlements = mysqlReader.load()

    /* loading this:
      +---------+-----------+-----------------+----------------+
      |givenname|    surname|            email|entitlementtoken|
      +---------+-----------+-----------------+----------------+
      |     Matt| Ingenthron|   matt@email.com|           11211|
      |  Michael|Nitschinger|michael@email.com|           11210|
      +---------+-----------+-----------------+----------------+
     */

    val entitlementsSansSchema = entitlements.rdd.map[(String, Integer)](f => (f.getAs[String]("email"), f.getAs[Integer]("entitlementtoken")))

    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.couchbaseStream("transformative")
      .filter(_.isInstanceOf[Mutation])
      .map(m => (new String(m.asInstanceOf[Mutation].key), new String(m.asInstanceOf[Mutation].content)))
      .map(s => CreateMappableRdd(s))
      .filter(_._2.content().get("entitlementtoken").eq(null))
      .foreachRDD(rdd => {
        rdd
          .join(entitlementsSansSchema)
          .map(mergeIntoDoc)
          //.foreach(println) // a good place to see the effect
          .saveToCouchbase("transformative")
      })

    ssc.start()
    ssc.awaitTermination()
  }

}


