package d2k.common.df

import org.apache.spark.sql.DataFrame
import d2k.common.InputArgs

trait Executor {
  def invoke(df: DataFrame)(implicit inArgs: InputArgs): DataFrame
}
