package transform

//import dataevents.data_cleaning.convertToLowercase
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ConvertToLowercase {
  def convertToLowercase(df1rename:DataFrame,df2rename:DataFrame):(DataFrame,DataFrame)={
    val df1lowercase = df1rename.withColumn("redirection_source", lower(col("redirection_source")))
    val df2lowercase = df2rename.withColumn("department_name", lower(col("department_name")))
    (df1lowercase,df2lowercase)
  }
}
