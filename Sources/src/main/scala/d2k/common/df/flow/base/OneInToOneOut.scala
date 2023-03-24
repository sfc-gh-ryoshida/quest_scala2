package d2k.common.df.flow.base

import d2k.common.InputArgs

trait OneInToOneOut[IN, PREOUT, MID, POSTIN, OUT] {
  def preExec(in: IN)(implicit inArgs: InputArgs): PREOUT

  def exec(df: MID)(implicit inArgs: InputArgs): MID

  def postExec(df: POSTIN)(implicit inArgs: InputArgs): OUT

  def run(in: IN)(implicit inArgs: InputArgs): OUT
}
