package d2k.common.df.template.base

import d2k.common.df.flow.TwoInToOneOutForDf
import d2k.common.InputArgs
import d2k.common.df.Executor
import com.snowflake.snowpark.DataFrame
import d2k.common.df.WritePq
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait TwoAnyToPq[IN1, IN2] extends TwoInToOneOutForDf[IN1, IN2, DataFrame] with WritePq {
   def postExec(df: DataFrame)(implicit inArgs: InputArgs) = writeParquet(df)
}