package transform

//import dataevents.data_cleaning.removeDuplicates
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RemoveDuplicates {
  def removeDuplicates(df1removenull:DataFrame,df2removenull:DataFrame):(DataFrame,DataFrame)={
    try {
      val df1Duplicates = df1removenull.dropDuplicates("id")
      val df2Duplicates = df2removenull.dropDuplicates("item_id")
      (df1Duplicates, df2Duplicates)
    } catch {
      case e: Exception =>
        println("An error occurred during duplicate removal:")
        e.printStackTrace()
        // You can handle the error here, such as returning default DataFrames or rethrowing the exception
        (df1removenull, df2removenull) // Returning original DataFrames as an example
    }
  }
}
