package transform

//import dataevents.data_cleaning.convertToLowercase
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ConvertToLowercase {
  def convertToLowercase(df1rename:DataFrame,df2rename:DataFrame):(DataFrame,DataFrame)={
    val df1lowercase = df1rename.withColumn("redirection_source_t", lower(col("redirection_source_t")))
    val df2lowercase = df2rename.withColumn("department_n", lower(col("department_n")))
    (df1lowercase,df2lowercase)
  }
}
