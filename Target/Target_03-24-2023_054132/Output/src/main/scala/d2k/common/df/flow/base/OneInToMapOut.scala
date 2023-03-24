package d2k.common.df.flow.base

import d2k.common.InputArgs
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait OneInToMapOut[IN, MID, OUT] extends OneInToOneOut[IN, Map[String, MID], MID, Map[String, MID], OUT] {
   def preExec(in: IN)(implicit inArgs: InputArgs): Map[String, MID]

   def exec(df: MID)(implicit inArgs: InputArgs): MID

   def postExec(df: Map[String, MID])(implicit inArgs: InputArgs): OUT

   def run(in: IN)(implicit inArgs: InputArgs): OUT
}