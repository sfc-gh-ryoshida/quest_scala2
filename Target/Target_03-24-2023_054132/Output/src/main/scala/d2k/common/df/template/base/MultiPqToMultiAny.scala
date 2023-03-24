package d2k.common.df.template.base

import d2k.common.df.flow.OneInToMapOutForDf
import d2k.common.df.Executor
import d2k.common.InputArgs
import com.snowflake.snowpark.DataFrame
import d2k.common.df.MultiReadPq
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait MultiPqToMultiAny[OUT] extends OneInToMapOutForDf[Unit, OUT] with MultiReadPq {
   def preExec(in: Unit)(implicit inArgs: InputArgs) : Map[String, DataFrame] = readParquet
}