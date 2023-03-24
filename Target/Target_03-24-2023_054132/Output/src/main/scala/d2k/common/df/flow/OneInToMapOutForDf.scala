/*
EWI: SPRKSCL1001 => This code section has parsing errors, so it was commented out
package d2k.common.df.flow

import org.apache.spark.sql.DataFrame
import d2k.common.Logging
import d2k.common.InputArgs
import d2k.common.df.flow.base.OneInToMapOut

trait OneInToMapOutForDf[IN, OUT] extends OneInToMapOut[IN, DataFrame, OUT] with Logging {
  def preExec(in: IN)(implicit inArgs: InputArgs): Map[String, DataFrame]

  def exec(df: DataFrame)(implicit inArgs: InputArgs): DataFrame

  def postExec(df: Map[String, DataFrame])(implicit inArgs: InputArgs): OUT

  final def run(in: IN)(implicit inArgs: InputArgs): OUT = {
    val input = try {
      preExec(in)
    } catch {
      case t: Throwable => platformError(t); throw t
    }

    if (inArgs.isDebug) {
      println(s"${inArgs.applicationId}[input]")
      input.foreach { data => println(data._1); data._2.show(false) }
    }

    val output = try {
      input.mapValues(exec)
    } catch {
      case t: Throwable => appError(t); throw t
    }

    if (inArgs.isDebug) {
      println(s"${inArgs.applicationId}[output]")
      output.foreach { data => println(data._1); data._2.show(false) }
    }

    try {
      postExec(output)
    } catch {
      case t: Throwable => platformError(t); throw t
    }
  }

  final def debug(in: IN)(implicit inArgs: InputArgs): OUT =
    run(in)(inArgs.copy(isDebug = true))
}

*/