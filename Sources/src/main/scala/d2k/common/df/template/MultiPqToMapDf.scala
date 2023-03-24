package d2k.common.df.template

import d2k.common.InputArgs
import org.apache.spark.sql.DataFrame
import d2k.common.df.template.base.MultiPqToMultiAny
import d2k.common.df.template.base.MultiAnyToMapDf
import d2k.common.df.Executor

trait MultiPqToMapDf extends MultiPqToMultiAny[Map[String, DataFrame]] with MultiAnyToMapDf[Unit] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
