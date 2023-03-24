package d2k.common.df.flow.base

import d2k.common.InputArgs
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait TwoInToOneOut[IN1, IN2, PREOUT, MID, POSTIN, OUT] {
   def preExec(in1: IN1, in2: IN2)(implicit inArgs: InputArgs): PREOUT

   def exec(df: MID)(implicit inArgs: InputArgs): MID

   def postExec(df: POSTIN)(implicit inArgs: InputArgs): OUT

   def run(in1: IN1, in2: IN2)(implicit inArgs: InputArgs): OUT
}