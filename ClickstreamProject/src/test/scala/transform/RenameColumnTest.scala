package transform

import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec

class RenameColumnTest extends AnyFlatSpec {
  val removeDuplicates= new RemoveDuplicatesTest
  var df1rename: DataFrame = _
  var df2rename: DataFrame = _
  "RenameColumn object" should "do the following"

  it should "rename columns" in{
    val result = CastDataTypes.castDataTypes(removeDuplicates.df1removeduplicates,removeDuplicates.df2removeduplicates)
    df1rename=result._1
    df2rename=result._2
    df1rename.show()
    df2rename.show()
  }
}
