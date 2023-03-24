package d2k.common.df.template.base

import d2k.common.df.flow.OneInToOneOutForDf
import d2k.common.InputArgs
import d2k.common.df.Executor
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.Row
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait AnyToVal[IN, T] extends OneInToOneOutForDf[IN, T] {
   def outputValue(rows: Array[Row]): T

   def postExec(df: DataFrame)(implicit inArgs: InputArgs) : T = outputValue(df.collect)
}