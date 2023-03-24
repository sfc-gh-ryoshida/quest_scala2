package d2k.common.df.executor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import d2k.common.df.Executor
import d2k.common.InputArgs
import d2k.common.df.executor._

trait DbOutputCommonFunctions extends Executor {
  def invoke(df: DataFrame)(implicit inArgs: InputArgs) = DbOutputCommonFunctions(df)
}

object DbOutputCommonFunctions {
  def apply(df: DataFrame)(implicit inArgs: InputArgs) =
    PqCommonColumnRemover(RowErrorRemover(df))
      .withColumn("VC_DISPOYMD", lit(inArgs.runningDateYMD))
}
