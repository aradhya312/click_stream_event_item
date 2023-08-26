package transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import service.DataPipeline

object NullCheck {
  def nullCheck(df1cast:DataFrame,df2cast:DataFrame):(DataFrame,DataFrame)={
      try {
      val df1notnull = df1cast.na.drop(Seq("id"))
      val df2notnull = df2cast.na.drop(Seq("item_id"))
        val placeholderClickstream = Map(
          "id" -> " ",
          "event_timestamp" -> "%",
          "device_type" -> "%",
          "session_id" -> "%",
          "visitor_id" -> "%",
          "item_id" -> "%",
          "redirection_source" -> "%"
        )
        val placeholderItemset = Map(
          "item_id" -> " ",
          "item_price" -> 0.0,
          "product_type" -> "%",
          "department_name" -> "%"
        )

        val replacedNullClickstream = df1notnull.na.fill(placeholderClickstream)
        val replacedNullItemset = df2notnull.na.fill(placeholderItemset)
        replacedNullClickstream.show()
        replacedNullItemset.show()

      val removedRecordsFromDf1 = df1cast.except(df1notnull)
      val removedRecordsFromDf2 = df2cast.except(df2notnull)

      val clickstreamnulls = ConfigFactory.load("application.conf").getString("output.clickstreamnullpath")
      val itemsetnulls = ConfigFactory.load("application.conf").getString("output.itemsetnullpath")

      removedRecordsFromDf1.repartition(1).write.option("header", "true").mode("overwrite").csv(clickstreamnulls)
      removedRecordsFromDf2.repartition(1).write.option("header", "true").mode("overwrite").csv(itemsetnulls)
      return (df1notnull, df2notnull)
    }
    catch {
      case ex: Exception =>
        DataPipeline.logger.error("An error occured due to failure of null removal. ", ex)
        (df1cast, df2cast)
    }
  }
}
