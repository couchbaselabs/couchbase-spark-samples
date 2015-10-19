import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.spark.japi.CouchbaseSparkContext;
import com.couchbase.spark.rdd.CouchbaseQueryRow;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

import static com.couchbase.spark.japi.CouchbaseDocumentRDD.couchbaseDocumentRDD;
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

        // Perform a N1QL query
        List<CouchbaseQueryRow> results = csc
            .couchbaseQuery(N1qlQuery.simple("SELECT * FROM `travel-sample` LIMIT 10"))
            .collect();

        System.out.println(results);

        // Store A (empty) Document
        couchbaseDocumentRDD(
            sc.parallelize(Arrays.asList(JsonDocument.create("doc1", JsonObject.empty())))
        ).saveToCouchbase();

    }
}
