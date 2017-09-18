import org.apache.spark.sql.SparkSession
import com.couchbase.spark._
import com.couchbase.spark.connection._

/**
  * Created by daschl on 13/07/17.
  */
object SubdocMutationExample {

  def main(args: Array[String]): Unit = {
    // The SparkSession is the main entry point into spark
    val spark = SparkSession
      .builder()
      .appName("SubdocExample")
      .master("local[*]") // use the JVM as the master, great for testing
      .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
      .config("spark.couchbase.bucket.travel-sample", "") // open the travel-sample bucket with empty password
      .config("spark.couchbase.username", "Administrator") // If you are using RBAC / Server 5.0
      .config("spark.couchbase.password", "password") // If you are using RBAC / Server 5.0
      .getOrCreate()




    // Change a field in a document that already exists
    spark.sparkContext
      .couchbaseSubdocMutate(Seq(SubdocReplace("airline_10", "name", "42-Mile-Air")))
      .collect()

    // Append an element to an array
    spark.sparkContext
      .couchbaseSubdocMutate(Seq(SubdocArrayAppend("airline_1191", "codes", 1, true)))
      .collect()

  }
}
