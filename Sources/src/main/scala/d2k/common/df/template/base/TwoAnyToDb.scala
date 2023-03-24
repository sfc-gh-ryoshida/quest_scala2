package d2k.common.df.template.base

import d2k.common.df.flow.TwoInToOneOutForDf
import d2k.common.InputArgs
import d2k.common.df.Executor
import org.apache.spark.sql.DataFrame
import d2k.common.df.WriteDb

trait TwoAnyToDb[IN1, IN2] extends TwoInToOneOutForDf[IN1, IN2, DataFrame] with WriteDb {
  def postExec(df: DataFrame)(implicit inArgs: InputArgs) = writeDb(df)
}
