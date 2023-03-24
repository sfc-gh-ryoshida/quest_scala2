package d2k.appdefdoc.gen.dic

object DictionaryGenerator extends App {
  val (baseUrl, branch) = (args(0), args(1))
  println(s"[Start Dictionary Generate] ${args.mkString(" ")}")
  GenerateDictionary(baseUrl).generate(branch)
}
