package transform

//import dataevents.data_cleaning.removeDuplicates
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RemoveDuplicates {
  def removeDuplicates(df1removenull:DataFrame,df2removenull:DataFrame):(DataFrame,DataFrame)={
    val df1Duplicates = df1removenull.dropDuplicates("id")
    val df2Duplicates = df2removenull.dropDuplicates("item_id")
    (df1Duplicates,df2Duplicates)
  }
}
