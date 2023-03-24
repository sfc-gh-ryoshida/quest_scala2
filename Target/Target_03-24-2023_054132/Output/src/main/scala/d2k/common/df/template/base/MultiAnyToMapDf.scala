package d2k.common.df.template.base

import d2k.common.df.flow.OneInToMapOutForDf
import d2k.common.InputArgs
import d2k.common.df.Executor
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.Row
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait MultiAnyToMapDf[IN] extends OneInToMapOutForDf[IN, Map[String, DataFrame]] {
   def postExec(df: Map[String, DataFrame])(implicit inArgs: InputArgs) : Map[String, DataFrame] = df
}