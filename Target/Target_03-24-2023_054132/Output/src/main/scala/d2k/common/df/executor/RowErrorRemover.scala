package d2k.common.df.executor

import com.snowflake.snowpark.DataFrame
import d2k.common.InputArgs
import com.snowflake.snowpark.functions._
import d2k.common.df.Executor
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait RowErrorRemover extends Executor {
   def invoke(df: DataFrame)(implicit inArgs: InputArgs) : DataFrame = RowErrorRemover(df)
}
 object RowErrorRemover {
   def apply(df: DataFrame)(implicit inArgs: InputArgs) = df.filter(col("ROW_ERR") === lit("false"))
}