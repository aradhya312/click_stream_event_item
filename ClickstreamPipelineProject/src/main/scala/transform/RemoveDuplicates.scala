package transform

//import dataevents.data_cleaning.removeDuplicates
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RemoveDuplicates {
  def removeDuplicates():(DataFrame,DataFrame)={
    val (df1notnull,df2notnull)=NullCheck.nullCheck()
    val df1Duplicates = df1notnull.dropDuplicates("Entity_id")
    val df2Duplicates = df2notnull.dropDuplicates("item_id")
    (df1Duplicates,df2Duplicates)
  }
}
