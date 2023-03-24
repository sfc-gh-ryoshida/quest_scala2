package d2k.common.df.flow

import com.snowflake.snowpark.DataFrame
import d2k.common.Logging
import d2k.common.InputArgs
import d2k.common.df.flow.base.OneInToOneOut
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait OneInToOneOutForDf[IN, OUT] extends OneInToOneOut[IN, DataFrame, DataFrame, DataFrame, OUT] with Logging {
   def preExec(in: IN)(implicit inArgs: InputArgs): DataFrame

   def exec(df: DataFrame)(implicit inArgs: InputArgs): DataFrame

   def postExec(df: DataFrame)(implicit inArgs: InputArgs): OUT

   final def run(in: IN)(implicit inArgs: InputArgs) : OUT = {
   val input = try
         {preExec(in)}
      catch {
         case t:Throwable => platformError(t);throw t
      }
if (inArgs.isDebug)
         {
         println(s"${ inArgs.applicationId }[input]")
input.show(false)
         }
 val output = try
         {exec(input)}
      catch {
         case t:Throwable => appError(t);throw t
      }
if (inArgs.isDebug)
         {
         println(s"${ inArgs.applicationId }[output]")
output.show(false)
         }
try
         {postExec(output)}
      catch {
         case t:Throwable => platformError(t);throw t
      }
   }

   final def debug(in: IN)(implicit inArgs: InputArgs) : OUT = run(in)(inArgs.copy(isDebug = true))
}