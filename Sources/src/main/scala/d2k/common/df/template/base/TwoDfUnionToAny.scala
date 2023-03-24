package d2k.common.df.template.base

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import d2k.common.InputArgs
import d2k.common.df.flow.TwoInToOneOutForDf

trait TwoDfUnionToAny[OUT] extends TwoInToOneOutForDf[DataFrame, DataFrame, OUT] {
  def preExec(left: DataFrame, right: DataFrame)(implicit inArgs: InputArgs): DataFrame =
    left.union(right)
}
