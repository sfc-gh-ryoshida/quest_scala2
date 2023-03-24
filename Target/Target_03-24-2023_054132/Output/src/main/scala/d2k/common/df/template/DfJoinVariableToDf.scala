package d2k.common.df.template

import com.snowflake.snowpark.DataFrame
import d2k.common.df.Executor
import d2k.common.InputArgs
import d2k.common.df.template.base.AnyToDf
import d2k.common.df.template.base.DfJoinVariableToAny
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait DfJoinVariableToDf extends DfJoinVariableToAny[DataFrame] with AnyToDf[DataFrame] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}