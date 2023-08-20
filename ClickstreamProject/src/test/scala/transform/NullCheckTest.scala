package transform

import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec

class NullCheckTest extends AnyFlatSpec {
  val castDataTypes= new CastDataTypesTest
  var df1removenull: DataFrame = _
  var df2removenull: DataFrame = _
  "NullCheck object" should "do the following"

  it should "remove null values" in{
    val result = NullCheck.nullCheck(castDataTypes.df1cast,castDataTypes.df2cast)
    df1removenull=result._1
    df2removenull=result._2
    df1removenull.show()
    df2removenull.show()
  }
}
