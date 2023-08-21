<<<<<<< HEAD
import service.{DataPipeline, FileReader, FileWriter}

=======
import database.DatabaseWrite
import service.{DataPipeline, FileReader, FileWriter}

import java.sql.Connection
import java.sql.DriverManager

>>>>>>> origin/feat_ayushi
object ClickstreamPipeLine {
  def main(args: Array[String]): Unit = {
    DataPipeline.dataPipeline()

  }
}