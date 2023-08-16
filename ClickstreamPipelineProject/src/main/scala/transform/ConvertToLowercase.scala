package transform

//import dataevents.data_cleaning.convertToLowercase
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ConvertToLowercase {
  def convertToLowercase():(DataFrame,DataFrame)={
    val (df1Duplicates,df2Duplicates)=RemoveDuplicates.removeDuplicates()
    val df1lowercase = df1Duplicates.withColumn("redirection_source_t", lower(col("redirection_source_t")))
    val df2lowercase = df2Duplicates.withColumn("department_n", lower(col("department_n")))
    (df1lowercase,df2lowercase)
  }
}
