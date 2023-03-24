package d2k.common.df.executor

import d2k.common.df.Executor
import com.snowflake.snowpark.DataFrame
import d2k.common.InputArgs
import com.snowflake.snowpark.functions._
import d2k.common.fileConv.Converter
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait PqCommonColumnRemover extends Executor {
   def invoke(df: DataFrame)(implicit inArgs: InputArgs) : DataFrame = PqCommonColumnRemover(df)
}
 object PqCommonColumnRemover {
   def apply(df: DataFrame)(implicit inArgs: InputArgs) = df.drop(Converter.SYSTEM_COLUMN_NAME.ROW_ERROR).drop(Converter.SYSTEM_COLUMN_NAME.ROW_ERROR_MESSAGE)
}