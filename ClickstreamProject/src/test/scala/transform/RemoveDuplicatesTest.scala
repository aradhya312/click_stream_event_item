package transform

import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec

class RemoveDuplicatesTest extends AnyFlatSpec {
  val nullCheck= new NullCheckTest
  var df1removeduplicates: DataFrame = _
  var df2removeduplicates: DataFrame = _
  "RemoveDuplicates object" should "do the following"

  it should "remove duplicate values" in{
    val result = RemoveDuplicates.removeDuplicates(nullCheck.df1removenull,nullCheck.df2removenull)
    df1removeduplicates=result._1
    df2removeduplicates=result._2
    df1removeduplicates.show()
    df2removeduplicates.show()
  }
}
