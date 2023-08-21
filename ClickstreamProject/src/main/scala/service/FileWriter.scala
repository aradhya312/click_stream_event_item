package service

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import transform.ConvertToLowercase

object FileWriter {
  def fileWriter(df1lowercase:DataFrame, df2lowercase:DataFrame,outputPath:String): DataFrame = {
    try {
      // Join both the dataframes
      df1lowercase.printSchema()
      df2lowercase.printSchema()
      val finalDF: DataFrame = df1lowercase.join(df2lowercase, Seq("item_id"))

      // Show the final DataFrame
      finalDF.show()

      // Write the final dataframe to a csv file
      finalDF.repartition(1).write.option("header", "true").csv(outputPath)
      finalDF
    } catch {
      case e: Exception =>
        println(s"An error occurred while writing the DataFrame to $outputPath:")
        e.printStackTrace()
        // You can handle the error here, such as returning an empty DataFrame or rethrowing the exception
        df1lowercase // Returning one of the input DataFrames as an example
    }
  }
}
