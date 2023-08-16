package service

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import transform.ConvertToLowercase

object DataPipeline {
  def dataPipeline():Unit={

  FileWriter.fileWriter()
  }
}
