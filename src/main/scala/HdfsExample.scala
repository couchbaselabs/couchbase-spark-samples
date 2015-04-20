import com.couchbase.client.java.document.JsonDocument
import org.apache.spark.{SparkContext, SparkConf}

object HdfsExample {

  def main(args: Array[String]): Unit = {

    // Configure Spark
    val cfg = new SparkConf()
      .setAppName("keyValueExample")
      .setMaster("local[*]")
      .set("com.couchbase.bucket.default", "")

    // Generate The Context
    val sc = new SparkContext(cfg)

    sc
      .textFile("hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/sample_07/*")
      .collect()
      .foreach(println)

  }
}
