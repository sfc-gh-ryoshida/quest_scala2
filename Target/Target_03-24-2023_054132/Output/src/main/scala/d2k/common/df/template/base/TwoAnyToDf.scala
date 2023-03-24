package d2k.common.df.template.base

import d2k.common.df.flow.TwoInToOneOutForDf
import d2k.common.InputArgs
import d2k.common.df.Executor
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait TwoAnyToDf[IN1, IN2] extends TwoInToOneOutForDf[IN1, IN2, DataFrame] {
   def postExec(df: DataFrame)(implicit inArgs: InputArgs) = df
}