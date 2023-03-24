package d2k.appdefdoc.parser

import scala.util.parsing.combinator.JavaTokenParsers

trait ComponentFlow {
  val componentId: String
}
case class CfId(componentId: String) extends ComponentFlow
case class Cf(componentId: String, desc: String) extends ComponentFlow
case object CfEnd extends ComponentFlow { val componentId = "CfEnd" }

case class CfPair(cf1: ComponentFlow, cf2: ComponentFlow)
case class CfData(pairs: Seq[CfPair])

object ComponentFlowParser extends JavaTokenParsers with D2kParser {
  def apply(target: String) = {
    parseAll(componentFlow, target)
  }

  val eol = '\n'
  val num2 = "[0-9][0-9]".r
  val anyWords = ".*".r
  val anyWords2 = "^(?!\\|).*".r

  def componentFlow = rep(withLink) ^^ { case x => CfData(x) }

  def componentId = "[0-9\\w_]*".r
  def component = componentId ^^ { case a => CfId(a) }
  def componentWithAs = ("""".*?"""".r <~ "as") ~ componentId ^^ {
    case a ~ b => Cf(b, a.replaceAllLiterally("\"", "").split("\\\\n")(1))
  }
  def componentTail = "(*)" ^^ { case _ => CfEnd }

  def componentPattern = componentTail | componentWithAs | component
  def withLink = ((componentPattern <~ "-->") ~ componentPattern) <~ eol ^^ {
    case a ~ b => CfPair(a, b)
  }
}
