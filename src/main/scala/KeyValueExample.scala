import com.couchbase.client.java.document.JsonDocument
import org.apache.spark.{SparkContext, SparkConf}
import com.couchbase.spark._

object KeyValueExample {

  def main(args: Array[String]): Unit = {

    // Configure Spark
    val cfg = new SparkConf()
      .setAppName("keyValueExample")
      .setMaster("local[*]")
      .set("com.couchbase.bucket.travel-sample", "")

    // Generate The Context
    val sc = new SparkContext(cfg)

    sc
      .parallelize(Seq("airline_10123", "airline_10748")) // Define Document IDs
      .couchbaseGet[JsonDocument]() // Load them from Couchbase
      .map(_.content()) // extract the content
      .collect() // collect all data
      .foreach(println) // print it out
  }

}
