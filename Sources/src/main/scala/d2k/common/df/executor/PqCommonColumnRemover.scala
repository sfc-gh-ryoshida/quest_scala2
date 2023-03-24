package d2k.common.df.executor

import d2k.common.df.Executor
import org.apache.spark.sql.DataFrame
import d2k.common.InputArgs
import org.apache.spark.sql.functions._
import d2k.common.fileConv.Converter

trait PqCommonColumnRemover extends Executor {
  def invoke(df: DataFrame)(implicit inArgs: InputArgs): DataFrame = PqCommonColumnRemover(df)
}

object PqCommonColumnRemover {
  def apply(df: DataFrame)(implicit inArgs: InputArgs) =
    df.drop(Converter.SYSTEM_COLUMN_NAME.ROW_ERROR).drop(Converter.SYSTEM_COLUMN_NAME.ROW_ERROR_MESSAGE)
}
