import service.{DataPipeline, FileReader, FileWriter}

object ClickstreamPipeLine {
  def main(args: Array[String]): Unit = {
    DataPipeline.dataPipeline()

  }
}