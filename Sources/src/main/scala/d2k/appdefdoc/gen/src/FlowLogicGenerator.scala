package d2k.appdefdoc.gen.src

object FlowLogicGenerator {
  sealed trait Tree {
    def toFlow: String
  }

  case class Node(id: String, t: Tree) extends Tree {
    def toFlow = {
      s"${t.toFlow} ~>\n    c${id}.run"
    }
  }

  case class Leaf(id: String) extends Tree {
    def toFlow = s"c${id}.run"
  }

  case class Top(id: String) extends Tree {
    def toFlow = s"c${id}.run(Unit)"
  }

  case class Join(id: String, l: Tree, r: Tree) extends Tree {
    def toFlow = s"(${l.toFlow}, ${r.toFlow}) ~>\n    c${id}.run"
  }

  def apply(target: Seq[(String, String)]) = {
    val flowMap = target.foldLeft(Map.empty[String, Seq[String]]) { (l, r) =>
      val (a, b) = r
      l.updated(b, l.get(b).getOrElse(Seq.empty[String]) :+ a)
    }

    def conv(s: String): Tree = {
      val flowId = flowMap.get(s).getOrElse(Seq.empty[String])
      flowId.size match {
        case 0 => Top(s)
        case 1 => Node(s, conv(flowId.head))
        case 2 => Join(s, conv(flowId(0)), conv(flowId(1)))
      }
    }
    flowMap("CfEnd").map(e => conv(e).toFlow).mkString("\n\n")
  }
}