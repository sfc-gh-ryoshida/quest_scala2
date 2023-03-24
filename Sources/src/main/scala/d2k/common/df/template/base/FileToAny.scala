package d2k.common.df.template.base

import d2k.common.df.flow.OneInToOneOutForDf
import d2k.common.df.ReadFile
import d2k.common.df.Executor
import d2k.common.InputArgs
import org.apache.spark.sql.DataFrame

trait FileToAny[OUT] extends OneInToOneOutForDf[Unit, OUT] with ReadFile {
  def preExec(in: Unit)(implicit inArgs: InputArgs): DataFrame = readFile
}
