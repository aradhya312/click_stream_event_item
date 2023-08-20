package utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import service.FileReader

object sparksessiontest {
  def sparkSession():SparkSession= {

    val sparkConf = FileReader.readConfig()
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val logLevel = ConfigFactory.load("test_application.conf").getString("spark.logLevel")
    spark.sparkContext.setLogLevel(logLevel)
  spark
  }
}
