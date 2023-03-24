package d2k.common.df.template

import org.apache.spark.sql.DataFrame
import d2k.common.df.Executor
import d2k.common.InputArgs
import d2k.common.df.template.base.TwoDfJoinToAny
import d2k.common.df.template.base.TwoAnyToDf

trait DfJoinToDf extends TwoDfJoinToAny[DataFrame] with TwoAnyToDf[DataFrame, DataFrame] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
