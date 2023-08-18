import database.DatabaseWrite
import service.{DataPipeline, FileReader, FileWriter}

import java.sql.Connection
import java.sql.DriverManager

object ClickstreamPipeLine {
  def main(args: Array[String]): Unit = {
    DataPipeline.dataPipeline()

  }
}