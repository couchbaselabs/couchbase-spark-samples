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
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.twitter.TwitterUtils

import com.couchbase.spark._

object TwitterFeed {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.exit(1)
    }

    // Configure Spark
    val cfg = new SparkConf()
      .setAppName("TwitterFeed")
      .setMaster("local[*]")
      .set("com.couchbase.bucket.twitter", "")

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val ssc = new StreamingContext(cfg, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, Seq())

    val hashTags = stream

    hashTags.foreachRDD(rdd => {
      rdd
        .map(status => JsonDocument.create(status.getId.toString, JsonObject.create().put("text", status.getText)))
        .saveToCouchbase()

    })

    ssc.start()
    ssc.awaitTermination()

  }
}
