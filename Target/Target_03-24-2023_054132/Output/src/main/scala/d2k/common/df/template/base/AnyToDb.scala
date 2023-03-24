package d2k.common.df.template.base

import d2k.common.df.flow.OneInToOneOutForDf
import d2k.common.InputArgs
import d2k.common.df.Executor
import com.snowflake.snowpark.DataFrame
import d2k.common.df.WriteDb
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait AnyToDb[IN] extends OneInToOneOutForDf[IN, DataFrame] with WriteDb {
   def postExec(df: DataFrame)(implicit inArgs: InputArgs) = writeDb(df)
}