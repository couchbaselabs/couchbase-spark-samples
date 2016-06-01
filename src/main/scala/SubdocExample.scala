import org.apache.spark.{SparkConf, SparkContext}
import com.couchbase.spark._

object SubdocSample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DatasetSample")
      .set("com.couchbase.bucket.travel-sample", "")

    val sc = new SparkContext(conf)

    val result = sc
      .parallelize(Seq("airline_10123"))
      .couchbaseSubdocLookup(get = Seq("name", "iata"), exists = Seq("foobar"))
      .collect()

    val r2  = sc.couchbaseSubdocLookup(Seq("airline_10123"), Seq("name", "iata"))

    // Prints
    // SubdocLookupResult(
    //    airline_10123,0,Map(name -> Texas Wings, iata -> TQ),Map(foobar -> false)
    // )
    result.foreach(println)

    // Prints
    // SubdocLookupResult(
    //    airline_10123,0,Map(name -> Texas Wings, iata -> TQ),Map()
    // )
    r2.foreach(println)

  }
}