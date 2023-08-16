package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ConvertToLowercase {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ConvertToLowercase").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}
