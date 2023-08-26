package database

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import service.{DataPipeline, FileReader}
import DatabaseConnection._
import org.slf4j.LoggerFactory

object DatabaseWrite {
  //val joinedDF=FileReader.readJoinedDataSet()
  def writeToMySQL(dataFrame: DataFrame, tableName: String): Unit = {
    try {
      dataFrame.write
        .format("jdbc")
        .mode("overwrite")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", constant.jdbcUrl)
        .option("dbtable", tableName)
        .option("user", constant.jdbcUser)
        .option("password", constant.jdbcPassword)
        .save()
    }
    catch {
      case e: Exception =>
       DataPipeline.logger.error(s"An error occurred while loading the data to table $tableName", e)
    }
  }
  }
