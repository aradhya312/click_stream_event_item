import service.DataPipeline

object ClickstreamPipeLine {
  def main(args: Array[String]): Unit = {
    DataPipeline.dataPipeline()
  }
}