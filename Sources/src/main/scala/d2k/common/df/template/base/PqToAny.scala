package d2k.common.df.template.base

import d2k.common.df.flow.OneInToOneOutForDf
import d2k.common.df.Executor
import d2k.common.InputArgs
import org.apache.spark.sql.DataFrame
import d2k.common.df.SingleReadPq

trait PqToAny[OUT] extends OneInToOneOutForDf[Unit, OUT] with SingleReadPq {
  def preExec(in: Unit)(implicit inArgs: InputArgs): DataFrame = readParquet
}
