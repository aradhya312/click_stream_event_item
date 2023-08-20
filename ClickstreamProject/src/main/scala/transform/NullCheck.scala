package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object NullCheck {
  def nullCheck(df1cast:DataFrame,df2cast:DataFrame):(DataFrame,DataFrame)={
  val df1notnull=df1cast.na.drop(Seq("id"))
    val df2notnull=df2cast.na.drop(Seq("item_id"))
    (df1notnull,df2notnull)
  }
}
