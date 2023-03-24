package d2k.common.df.template.base

import d2k.common.df.flow.OneInToOneOutForDf
import d2k.common.InputArgs
import d2k.common.df.Executor
import com.snowflake.snowpark.DataFrame
import d2k.common.df.WriteFile
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait AnyToFile[IN] extends OneInToOneOutForDf[IN, DataFrame] with WriteFile {
   def postExec(df: DataFrame)(implicit inArgs: InputArgs) = writeFile(df)
}