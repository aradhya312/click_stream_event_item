package database

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import service.FileReader
import DatabaseConnection._

object DatabaseWrite {
  //val joinedDF=FileReader.readJoinedDataSet()
  def writeToMySQL(dataFrame: DataFrame, tableName: String): Unit = {
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
  }
