import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.query.Query
import com.couchbase.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.{SparkConf, SparkContext}
import com.couchbase.spark.sql._

object N1QLQueryExample {

  def main(args: Array[String]): Unit = {

    // Configure Spark
    val cfg = new SparkConf()
      .setAppName("keyValueExample")
      .setMaster("local[*]")
      .set("com.couchbase.bucket.travel-sample", "")

    // Generate The Context
    val sc = new SparkContext(cfg)

    val sql = new SQLContext(sc)

    // manual schema
    /*val df = sql.n1ql(StructType(
      StructField("name", StringType) ::
      StructField("abv", DoubleType) ::
      StructField("type", StringType) :: Nil
    ))*/

    // schema inference
    val df = sql.n1ql(filter = EqualTo("type", "airline"))

    df.printSchema()

    // SparkSQL Integration
//    df
//      .select("name", "callsign")
//      .sort(df("callsign").desc)
//      .show(10)

    // Plain old Spark emitting RDDs
//    sc.couchbaseQuery(Query.simple("SELECT * from `travel-sample`"))
//      .collect()
//      .foreach(println)
  }



}
