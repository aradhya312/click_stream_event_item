package transform

import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import service.FileWriter

class FileWriterTest extends AnyFlatSpec {
  val convertToLowercase= new ConvertToLowercaseTest
  var final_test_DF: DataFrame = _
  "FileWriter object" should "do the following"

  it should "join the two datasets" in{
    final_test_DF = FileWriter.fileWriter(convertToLowercase.df1lowercase,convertToLowercase.df2lowercase)
    final_test_DF.show()
  }
}
