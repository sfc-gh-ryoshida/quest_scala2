package d2k.common.df.template

import com.snowflake.snowpark.DataFrame
import d2k.common.df.Executor
import d2k.common.InputArgs
import d2k.common.df.template.base.TwoPqJoinToAny
import d2k.common.df.template.base.TwoAnyToPq
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait PqJoinToPq extends TwoPqJoinToAny[DataFrame] with TwoAnyToPq[Unit, Unit] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = PqJoinToPq.this.invoke(df)
}