package service

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import utils.sparksession
import org.apache.spark.sql.functions._

object FileReader {

  //Method to convert the output file to DF
//  def readJoinedDataSet():DataFrame= {
//    val spark = sparksession.sparkSession()
//    val OutPutPath = ConfigFactory.load("application.conf").getString("output.path")
//    val JoinedDF = spark.read.option("header", "true").option("inferSchema", "true").csv(OutPutPath)
//    JoinedDF
//  }

  //to read application config file
  def readConfig(): SparkConf = {
    val config = ConfigFactory.load("application.conf")
    val sparkAppName = config.getString("spark.appName")
    val sparkMaster = config.getString("spark.master")
    new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
  }

  //to read the input csv file
  def readDataFrame(spark:SparkSession, inputpath:String): DataFrame= {
    val dataframe=spark.read.option("header", "true").option("inferSchema", "true").csv(inputpath)
//    val spark=sparksession.sparkSession()
    // Read clickstream data from input paths
//    val inputPath1 = ConfigFactory.load("application.conf").getString("input.path1")
//    val inputPath2 = ConfigFactory.load("application.conf").getString("input.path2")


    // Read both CSV files into a DataFrame
//    val df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath1)
//    val df2 = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath2)
    dataframe
  }
}
