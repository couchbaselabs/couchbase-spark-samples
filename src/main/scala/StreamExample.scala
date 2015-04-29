import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import com.couchbase.spark.streaming._

object StreamExample {

  def main(args: Array[String]): Unit = {

    System.setProperty("com.couchbase.dcpEnabled", "true")

    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingSample")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc
      .couchbaseStream()
      .print()

    ssc.start()
    ssc.awaitTermination()
  }

}
