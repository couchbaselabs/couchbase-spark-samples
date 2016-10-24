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
import org.apache.spark.SparkContext
import com.couchbase.spark._
import java.io.File

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.SparkSession

/**
  * This example shows how to use the Machine Learning Word2Vec model together with Couchbase.
  *
  * It takes reviews from the travel-sample bucket, trains the model and then searches for
  * synonyms for "hotel".
  *
  * @author Will Gardella
  * @author Michael Nitschinger
  */
object Word2VecExample {

  def main(args: Array[String]): Unit = {

    // The SparkSession is the main entry point into spark
    val spark = SparkSession
      .builder()
      .appName("Word2VecExample")
      .master("local[*]") // use the JVM as the master, great for testing
      .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
      .config("spark.couchbase.bucket.travel-sample", "") // open the travel-sample bucket with empty password
      .getOrCreate()

    // Train the model if not trained already
    val model = trainAndLoadModel(spark.sparkContext)

    // Find the synonyms in the trained model and print them out
    val synonyms = model
      .findSynonyms(args.lift(0).getOrElse("hotel"), args.lift(1).map(_.toInt).getOrElse(10))
      .foreach { case (syn, sim) =>
        println(s"\t(•͡˘㇁•͡˘) --> \t[$syn] (with a similarity of $sim)")
      }

    spark.stop()
  }

  /**
    * Trains the model if no training data exists (based on the result of a n1ql query) and returns the trained
    * data (the model).
    */
  def trainAndLoadModel(sc: SparkContext): Word2VecModel = {
    val word2vec = new Word2Vec
    val path = "reviews"

    if (!new File(path).exists) {
      val reviews = "SELECT m.content from (" +
        "SELECT ELEMENT reviews FROM `travel-sample` WHERE type = 'hotel' AND ARRAY_LENGTH(reviews) > 0" +
        ") AS x UNNEST x AS m;"
      val input = sc.couchbaseQuery(N1qlQuery.simple(reviews))
        .map(_.value.getString("content").split(" ").map(_.toLowerCase.replaceAll("[^a-z0-9]", "")).toSeq)

      val m = word2vec.fit(input)
      m.save(sc, path)
      m
    } else {
      Word2VecModel.load(sc,  path)
    }
  }
}