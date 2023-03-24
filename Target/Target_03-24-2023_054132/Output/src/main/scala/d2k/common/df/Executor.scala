package d2k.common.df

import com.snowflake.snowpark.DataFrame
import d2k.common.InputArgs
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait Executor {
   def invoke(df: DataFrame)(implicit inArgs: InputArgs): DataFrame
}