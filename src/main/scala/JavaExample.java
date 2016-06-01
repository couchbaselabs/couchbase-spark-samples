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
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.spark.japi.CouchbaseSparkContext;
import com.couchbase.spark.rdd.CouchbaseQueryRow;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.EqualTo;

import java.util.Arrays;
import java.util.List;

import static com.couchbase.spark.japi.CouchbaseDataFrameReader.couchbaseReader;
import static com.couchbase.spark.japi.CouchbaseDocumentRDD.couchbaseDocumentRDD;
import static com.couchbase.spark.japi.CouchbaseRDD.couchbaseRDD;
import static com.couchbase.spark.japi.CouchbaseSparkContext.couchbaseContext;

public class JavaExample {

    public static void main(String[] args) {
        
        SparkConf conf = new SparkConf()
            .setAppName("javaSample")
            .setMaster("local[*]")
            .set("com.couchbase.bucket.travel-sample", "");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // The Couchbase-Enabled spark context
        CouchbaseSparkContext csc = couchbaseContext(sc);

        // Load docs through K/V
        List<JsonDocument> docs = csc
            .couchbaseGet(Arrays.asList("airline_10226", "airline_10748"))
            .collect();
        System.out.println(docs);

        // Load docs through K/V from a mapped RDD
        JavaRDD<String> ids = sc.parallelize(Arrays.asList("airline_10226", "airline_10748"));
        docs = couchbaseRDD(ids).couchbaseGet().collect();
        System.out.println(docs);

        // Perform a N1QL query
        List<CouchbaseQueryRow> results = csc
            .couchbaseQuery(N1qlQuery.simple("SELECT * FROM `travel-sample` LIMIT 10"))
            .collect();

        System.out.println(results);

        // Store A (empty) Document
        couchbaseDocumentRDD(
            sc.parallelize(Arrays.asList(JsonDocument.create("doc1", JsonObject.empty())))
        ).saveToCouchbase();

        // Use SparkSQL from Java
        SQLContext sql = new SQLContext(sc);

        // Wrap the Reader and create the DataFrame from Couchbase
        DataFrame airlines = couchbaseReader(sql.read()).couchbase(new EqualTo("type", "airline"));

        // Print the number of airline
        System.out.println("Number of Airlines: " + airlines.count());

    }
}
