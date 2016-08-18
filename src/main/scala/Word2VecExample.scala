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
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.couchbase.spark._


import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}


object Word2VecExample {

  def main(args: Array[String]): Unit = {

    // Configure Spark
    val cfg = new SparkConf()
      .setAppName("Word2VecExample")
      .setMaster("local[*]")
      .set("com.couchbase.bucket.travel-sample", "")

    // Generate the Context
    val sc = new SparkContext(cfg)

    // Setup Spark SQL
    val sql = new SQLContext(sc)

    // N1QL query gets hotel reviews from travel-sample bucket in Couchbase Server
    val reviews = "SELECT m.content from (SELECT ELEMENT reviews FROM `travel-sample` WHERE type = 'hotel' AND ARRAY_LENGTH(reviews) > 0) AS x UNNEST x AS m;"
    // Execute the query and clean the text before training the model
    val input = sc.couchbaseQuery(N1qlQuery.simple(reviews))
      .map(row => row.value.getString("content").split(" ").map(_.toLowerCase.replaceAll("[^a-z0-9]", "")).toSeq)

    // Uncomment this alternative to the N1QL query to train a model from a file instead
    // val input = sc.textFile("../datasets/reviews.txt").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()

    val pathToModels = "target/generatedModels/reviews"
    val model = if (! new java.io.File(pathToModels).exists) {
      // Train and save a new model
      // Delete the directory with the generated models if you want to retrain
      val m = word2vec.fit(input)
      m.save(sc, pathToModels)
      m
    } else {
      // Load a model you trained previously
      Word2VecModel.load(sc,  pathToModels)
    }

    // val synonyms = model.findSynonyms("hotel", 5)
    // Uncomment to pass args, e.g. sbt "run-main Word2VecExample hotel 5"
     val synonyms = model.findSynonyms(args(0), args(1).toInt)

   for((synonym, cosineSimilarity) <- synonyms) {
      println(s"  Look at this!    (•͡˘㇁•͡˘) ------> $synonym $cosineSimilarity")
    }

    sc.stop()
  }
}
