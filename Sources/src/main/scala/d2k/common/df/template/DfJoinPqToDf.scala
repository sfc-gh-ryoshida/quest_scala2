package d2k.common.df.template

import org.apache.spark.sql.DataFrame
import d2k.common.df.Executor
import d2k.common.InputArgs
import d2k.common.df.template.base.DfJoinMultiPqToAny
import d2k.common.df.template.base.AnyToDf

trait DfJoinPqToDf extends DfJoinMultiPqToAny[DataFrame] with AnyToDf[DataFrame] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
