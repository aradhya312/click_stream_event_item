package service

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FileReader {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("FileReader").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}
