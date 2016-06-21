import com.couchbase.client.java.document.JsonDocument
import org.apache.spark.{SparkConf, SparkContext}
import com.couchbase.spark._

object MultiBucketExample {

    def main(args: Array[String]): Unit = {
      // Configure Spark
      val cfg = new SparkConf()
        .setAppName("keyValueExample")
        .setMaster("local[*]")
        .set("com.couchbase.bucket.default", "")
        .set("com.couchbase.bucket.travel-sample", "")

      // Generate The Context
      val sc = new SparkContext(cfg)

      sc
        .parallelize(Seq("airline_10123", "airline_10748")) // Define Document IDs
        .couchbaseGet[JsonDocument](bucketName = "travel-sample") // Load them from travel-sample
        .saveToCouchbase(bucketName = "default") // write them into default
    }
}
