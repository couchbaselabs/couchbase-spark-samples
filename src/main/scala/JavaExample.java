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
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.spark.japi.CouchbaseSparkContext;
import com.couchbase.spark.rdd.CouchbaseQueryRow;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.EqualTo;

import java.util.Arrays;
import java.util.List;

import static com.couchbase.spark.japi.CouchbaseDataFrameReader.couchbaseReader;
import static com.couchbase.spark.japi.CouchbaseDocumentRDD.couchbaseDocumentRDD;
import static com.couchbase.spark.japi.CouchbaseRDD.couchbaseRDD;
import static com.couchbase.spark.japi.CouchbaseSparkContext.couchbaseContext;

/**
 * This example shows how to use Spark and the Couchbase Connector from Java.
 *
 * @author Michael Nitschinger
 */
public class JavaExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
            .builder()
            .appName("JavaExample")
            .master("local[*]") // use the JVM as the master, great for testing
            .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
            .config("spark.couchbase.bucket.travel-sample", "") // open the travel-sample bucket with empty password
            .getOrCreate();

        // The Java wrapper around the SparkContext
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // The Couchbase-Enabled SparkContext
        CouchbaseSparkContext csc = couchbaseContext(jsc);

        // Load docs through K/V
        List<JsonDocument> docs = csc
            .couchbaseGet(Arrays.asList("airline_10226", "airline_10748"))
            .collect();
        System.out.println(docs);

        // Load docs through K/V from a mapped RDD
        JavaRDD<String> ids = jsc.parallelize(Arrays.asList("airline_10226", "airline_10748"));
        docs = couchbaseRDD(ids).couchbaseGet().collect();
        System.out.println(docs);

        // Perform a N1QL query
        List<CouchbaseQueryRow> results = csc
            .couchbaseQuery(N1qlQuery.simple("SELECT * FROM `travel-sample` LIMIT 10"))
            .collect();

        System.out.println(results);

        // Store A (empty) Document
        couchbaseDocumentRDD(
            jsc.parallelize(Arrays.asList(JsonDocument.create("doc1", JsonObject.empty())))
        ).saveToCouchbase();

        // Wrap the Reader and create the DataFrame from Couchbase
        // Note that since Spark 2.0, a DataFrame == Dataset<Row>
        Dataset<Row> airlines = couchbaseReader(spark.read()).couchbase(new EqualTo("type", "airline"));

        // Print the Inferred Schema
        airlines.printSchema();

        // Print the number of airline
        System.out.println("Number of Airlines: " + airlines.count());

    }
}
