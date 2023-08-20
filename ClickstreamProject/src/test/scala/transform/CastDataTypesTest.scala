package transform

import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import service.FileReaderTest

class CastDataTypesTest extends AnyFlatSpec {
  val fileReader= new FileReaderTest
  var df1cast: DataFrame = _
  var df2cast: DataFrame = _
  "CastDataTypes object" should "do the following"

  it should "cast datatypes of columns to desired datatypes" in{
    val result = CastDataTypes.castDataTypes(fileReader.clickstream_test_DF,fileReader.itemset_test_DF)
    df1cast=result._1
    df2cast=result._2
    df1cast.show()
    df2cast.show()
  }
}
