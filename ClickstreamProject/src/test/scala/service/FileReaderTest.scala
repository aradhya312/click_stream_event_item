package service

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import utils.sparksessiontest

class FileReaderTest extends AnyFlatSpec {
  var clickstream_test_DF: DataFrame = _
  var itemset_test_DF: DataFrame = _
  "FileReader object" should "do the following"

  it should "read the files in the path" in {
    val spark = sparksessiontest.sparkSession()
    clickstream_test_DF = FileReader.readDataFrame(spark,"input.sample_path1")
    itemset_test_DF = FileReader.readDataFrame(spark,"input.sample_path2")

    assert(clickstream_test_DF.count()>=1)
    assert(itemset_test_DF.count()>=1)

    clickstream_test_DF.show()
    itemset_test_DF.show()
  }
}
