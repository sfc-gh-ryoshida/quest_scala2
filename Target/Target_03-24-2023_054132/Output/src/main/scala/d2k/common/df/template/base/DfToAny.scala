package d2k.common.df.template.base

import d2k.common.df.flow.OneInToOneOutForDf
import d2k.common.df.Executor
import d2k.common.InputArgs
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait DfToAny[OUT] extends OneInToOneOutForDf[DataFrame, OUT] {
   def preExec(in: DataFrame)(implicit inArgs: InputArgs) : DataFrame = in
}