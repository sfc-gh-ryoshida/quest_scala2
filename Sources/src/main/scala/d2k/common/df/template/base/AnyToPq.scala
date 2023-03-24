package d2k.common.df.template.base

import d2k.common.df.flow.OneInToOneOutForDf
import d2k.common.InputArgs
import d2k.common.df.Executor
import org.apache.spark.sql.DataFrame
import d2k.common.df.WritePq

trait AnyToPq[IN] extends OneInToOneOutForDf[IN, DataFrame] with WritePq {
  def postExec(df: DataFrame)(implicit inArgs: InputArgs) = writeParquet(df)
}
