package d2k.appdefdoc.gen.test

object TestGenerator extends App {
  val (baseUrl, branch, appGroup, appId) = (args(0), args(1), args(2), args(3))
  println(s"[Start Test Case Generate] ${args.mkString(" ")}")
  GenerateTestCase(baseUrl).generate(branch, appGroup, appId).write()
}
