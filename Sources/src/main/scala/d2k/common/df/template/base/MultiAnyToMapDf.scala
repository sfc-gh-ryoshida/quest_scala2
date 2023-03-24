package d2k.common.df.template.base

import d2k.common.df.flow.OneInToMapOutForDf
import d2k.common.InputArgs
import d2k.common.df.Executor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

trait MultiAnyToMapDf[IN] extends OneInToMapOutForDf[IN, Map[String, DataFrame]] {
  def postExec(df: Map[String, DataFrame])(implicit inArgs: InputArgs): Map[String, DataFrame] = df
}
