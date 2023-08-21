package service

import database.DatabaseWrite
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import transform.ConvertToLowercase

object DataPipeline {
  def dataPipeline():Unit={

<<<<<<< HEAD
  //FileWriter.fileWriter()

    val joinedDF = FileWriter.fileWriter()
    DatabaseWrite.writeToMySQL(joinedDF, "cdp")
=======
  FileWriter.fileWriter()

//    val joinedDF = FileWriter.fileWriter()
//    DatabaseWrite.writeToMySQL(joinedDF, "cdp")
>>>>>>> origin/feat_ayushi
  }
}
