package database

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object constant {
  val jdbcUrl = "jdbc:mysql://localhost:3306/clickstreamdata"
  val jdbcUser = "root"
  val jdbcPassword = "0702@Sidd"
}
