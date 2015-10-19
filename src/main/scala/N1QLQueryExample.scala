import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.query.N1qlQuery
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

    // Spark SQL Setup
    val sql = new SQLContext(sc)

    // Plain old Spark emitting RDDs from a N1QL Query result
    sc.couchbaseQuery(N1qlQuery.simple("SELECT * from `travel-sample` LIMIT 10"))
      .collect()
      .foreach(println)

    // Create a DataFrame with Schema Inference
    val df = sql.read.couchbase(schemaFilter = EqualTo("type", "airline"))

    // A DataFrame can also be created from an explicit schema
    /*val df = sql.n1ql(StructType(
      StructField("name", StringType) ::
      StructField("abv", DoubleType) ::
      StructField("type", StringType) :: Nil
    ))*/

    // Print The Schema
    df.printSchema()

    // SparkSQL Integration
    df
      .select("name", "callsign")
      .sort(df("callsign").desc)
      .show(10)


  }



}
