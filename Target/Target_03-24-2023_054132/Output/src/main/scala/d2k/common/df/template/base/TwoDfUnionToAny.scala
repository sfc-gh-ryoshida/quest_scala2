package d2k.common.df.template.base

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.Column
import d2k.common.InputArgs
import d2k.common.df.flow.TwoInToOneOutForDf
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait TwoDfUnionToAny[OUT] extends TwoInToOneOutForDf[DataFrame, DataFrame, OUT] {
   def preExec(left: DataFrame, right: DataFrame)(implicit inArgs: InputArgs) : DataFrame = left.union(right)
}