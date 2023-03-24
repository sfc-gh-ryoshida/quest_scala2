package d2k.common.df.executor

import d2k.common.df.Executor
import org.apache.spark.sql.DataFrame
import d2k.common.InputArgs
import org.apache.spark.sql.functions._
import d2k.common.fileConv.Converter

trait Nothing extends Executor {
  def invoke(df: DataFrame)(implicit inArgs: InputArgs): DataFrame = df
}
