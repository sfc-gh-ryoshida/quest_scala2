package d2k.common.df.template

import com.snowflake.snowpark.DataFrame
import d2k.common.df.Executor
import d2k.common.InputArgs
import d2k.common.df.template.base.TwoDfJoinToAny
import d2k.common.df.template.base.TwoAnyToDf
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait DfJoinToDf extends TwoDfJoinToAny[DataFrame] with TwoAnyToDf[DataFrame, DataFrame] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}