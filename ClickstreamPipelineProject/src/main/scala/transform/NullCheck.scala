package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object NullCheck {
  def nullCheck():(DataFrame,DataFrame)={
    val (df1rename,df2rename)=RenameColumn.renameColumn()
  val df1notnull=df1rename.na.drop(Seq("Entity_id"))
    val df2notnull=df2rename.na.drop(Seq("item_id"))
    (df1notnull,df2notnull)
  }
}
