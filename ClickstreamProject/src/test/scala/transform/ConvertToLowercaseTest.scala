package transform

import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec

class ConvertToLowercaseTest extends AnyFlatSpec {
  val renameColumn= new RenameColumnTest
  var df1lowercase: DataFrame = _
  var df2lowercase: DataFrame = _
  "ConvertToLowercase object" should "do the following"

  it should "convert the specified column records to lowercase" in{
    val result = ConvertToLowercase.convertToLowercase(renameColumn.df1rename,renameColumn.df2rename)
    df1lowercase=result._1
    df2lowercase=result._2
    df1lowercase.show()
    df2lowercase.show()
  }
}
