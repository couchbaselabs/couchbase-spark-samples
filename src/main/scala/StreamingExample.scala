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
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import com.couchbase.spark.streaming._

/**
  * This example shows how to perform raw Spark Streaming from a Couchbase DCP feed.
  *
  * If you are looking for streaming structured data more easily, take a look at the newly introduced
  * [[StructuredStreamingExample]] instead, which is also easier to use and provides stronger
  * guarantees out of the box.
  *
  * @author Michael Nitschinger
  */
object StreamingExample {

  def main(args: Array[String]): Unit = {

    // Create the Spark Config and instruct to use the travel-sample bucket
    // with no password.
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("StreamingExample")
      .set("com.couchbase.bucket.travel-sample", "")

    // Initialize StreamingContext with a Batch interval of 5 seconds
    val ssc = new StreamingContext(conf, Seconds(5))

    // Consume the DCP Stream from the beginning and never stop.
    // This counts the messages per interval and prints their count.
    ssc
      .couchbaseStream(from = FromBeginning, to = ToInfinity)
      .count()
      .print()

    // Start the Stream and await termination
    ssc.start()
    ssc.awaitTermination()
  }

}
