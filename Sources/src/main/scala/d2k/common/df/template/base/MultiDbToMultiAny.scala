package d2k.common.df.template.base

import d2k.common.df.flow.OneInToMapOutForDf
import d2k.common.df.Executor
import d2k.common.InputArgs
import org.apache.spark.sql.DataFrame
import d2k.common.df.MultiReadDb

trait MultiDbToMultiAny[OUT] extends OneInToMapOutForDf[Unit, OUT] with MultiReadDb {
  def preExec(in: Unit)(implicit inArgs: InputArgs): Map[String, DataFrame] = readDb
}
