package d2k.common.df.executor

import org.apache.spark.sql.DataFrame
import d2k.common.InputArgs
import org.apache.spark.sql.functions._
import d2k.common.df.Executor

trait RowErrorRemover extends Executor {
  def invoke(df: DataFrame)(implicit inArgs: InputArgs): DataFrame = RowErrorRemover(df)
}

object RowErrorRemover {
  def apply(df: DataFrame)(implicit inArgs: InputArgs) = df.filter(col("ROW_ERR") === lit("false"))
}
