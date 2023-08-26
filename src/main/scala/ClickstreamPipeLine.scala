import database.DatabaseWrite
import service.{DataPipeline, FileReader, FileWriter}

import java.sql.Connection
import java.sql.DriverManager

object ClickstreamPipeLine {
  def main(args: Array[String]): Unit = {
    try {
      DataPipeline.dataPipeline()
    }
    catch{
      case e: Exception=>
        DataPipeline.logger.error("An error occured due to failure of  main function",e)
    }
  }
}