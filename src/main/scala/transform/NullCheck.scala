package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object NullCheck {
  def nullCheck(df1cast:DataFrame,df2cast:DataFrame):(DataFrame,DataFrame)={

    var df1notnull: DataFrame = null
    var df2notnull: DataFrame = null

    try {
      val df1notnull = df1cast.na.drop(Seq("id"))
      val df2notnull = df2cast.na.drop(Seq("item_id"))
      return (df1notnull, df2notnull)
    }
    catch {
      case ex: Exception => println(s"An exception occurred: ${ex.getMessage}")
        df1notnull = null
        df2notnull = null
    }

    if (df1notnull != null && df2notnull != null) {
      (df1notnull, df2notnull)
    } else {
      (null, null)
    }

  }
}
