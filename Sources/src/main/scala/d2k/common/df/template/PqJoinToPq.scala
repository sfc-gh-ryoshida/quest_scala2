package d2k.common.df.template

import org.apache.spark.sql.DataFrame
import d2k.common.df.Executor
import d2k.common.InputArgs
import d2k.common.df.template.base.TwoPqJoinToAny
import d2k.common.df.template.base.TwoAnyToPq

trait PqJoinToPq extends TwoPqJoinToAny[DataFrame] with TwoAnyToPq[Unit, Unit] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = PqJoinToPq.this.invoke(df)
}
