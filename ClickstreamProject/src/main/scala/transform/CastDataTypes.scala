package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import service.FileReader

object CastDataTypes {
  def castDataTypes(df1:DataFrame,df2:DataFrame):(DataFrame,DataFrame)={

    val df1cast = df1.withColumn("event_timestamp",
      to_timestamp(col("event_timestamp"), "MM/dd/yyyy HH:mm"))
    val df2cast: DataFrame = df2.withColumn("item_price", col("item_price").cast("Double"))
    (df1cast,df2cast)
  }
}
