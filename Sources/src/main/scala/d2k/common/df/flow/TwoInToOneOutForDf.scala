package d2k.common.df.flow

import org.apache.spark.sql.DataFrame
import d2k.common.Logging
import d2k.common.InputArgs
import d2k.common.df.flow.base.TwoInToOneOut

trait TwoInToOneOutForDf[IN1, IN2, OUT]
    extends TwoInToOneOut[IN1, IN2, DataFrame, DataFrame, DataFrame, OUT] with Logging {
  def preExec(in1: IN1, in2: IN2)(implicit inArgs: InputArgs): DataFrame

  def exec(df: DataFrame)(implicit inArgs: InputArgs): DataFrame

  def postExec(df: DataFrame)(implicit inArgs: InputArgs): OUT

  final def run(in1: IN1, in2: IN2)(implicit inArgs: InputArgs): OUT = {
    val input = try {
      preExec(in1, in2)
    } catch {
      case t: Throwable => platformError(t); throw t
    }

    if (inArgs.isDebug) {
      println(s"${inArgs.applicationId}[input]")
      input.show(false)
    }

    val output = try {
      exec(input)
    } catch {
      case t: Throwable => appError(t); throw t
    }

    if (inArgs.isDebug) {
      println(s"${inArgs.applicationId}[output]")
      output.show(false)
    }

    try {
      postExec(output)
    } catch {
      case t: Throwable => platformError(t); throw t
    }
  }

  final def debug(in1: IN1, in2: IN2)(implicit inArgs: InputArgs): OUT =
    run(in1, in2)(inArgs.copy(isDebug = true))
}
