package d2k.appdefdoc.gen.src

import scala.util.matching.Regex.Match
import scala.util.matching.Regex

object ItemReplace {
  def addDq(s: String) = s""""$s""""
  val regxWithParent = """(\S+)\[[^\[\]]*\]\s(_\S+)\[[^\[\]]*\]""".r
  val regx = """[\"\'](.*?)[\"\']\((.*?)\)""".r
  val regxNull = """[Nn][Uu][Ll][Ll]\((.*?)\)""".r
  val regxDecimal = """\d+\((.*?)\)""".r
  val regxItem = """\S+\[(.*?)\]""".r
  val regxJoinLeft = """left\.\S+\[(.*?)\]""".r
  val regxJoinRight = """right\.\S+\[(.*?)\]""".r

  val runningDates = Seq(
    ("[運用日]", "inArgs.runningDate.MANG_DT"),
    ("[運用日(年)]", "inArgs.runningDate.MANG_DT.take(4)"),
    ("[運用日(年月)]", "inArgs.runningDate.MANG_DT.take(6)"),
    ("[運用日(日)]", "inArgs.runningDate.MANG_DT.takeRight(2)"),
    ("[前日]", "inArgs.runningDate.YST_DY"),
    ("[翌日]", "inArgs.runningDate.NXT_DT"),
    ("[前月]", "inArgs.runningDate.BEF_MO"),
    ("[前月_月初日]", "inArgs.runningDate.BEF_MO_FRST_MTH_DT"),
    ("[前月_月末日]", "inArgs.runningDate.BEF_MO_MTH_DT"),
    ("[当月]", "inArgs.runningDate.CURR_MO"),
    ("[当月_月初日]", "inArgs.runningDate.CURR_MO_FRST_MTH_DT"),
    ("[当月_月末日]", "inArgs.runningDate.CURR_MO_MTH_DT"),
    ("[翌月]", "inArgs.runningDate.NXT_MO"),
    ("[翌月_月初日]", "inArgs.runningDate.NXT_MO_FRST_MTH_DT"),
    ("[翌月_月末日]", "inArgs.runningDate.NXT_MO_MTH_DT"))

  def dropDescription(divKey: Char) =
    (_: Regex.Match).toString.split(divKey).dropRight(1).mkString(divKey.toString)

  object scala {
    def replaceLitStr(inStr1: String) = {
      val inStr = inStr1.split(",").map(_.trim).mkString("\"", "\", \"", "\"")
      val replaceBasic = Seq(
        regxWithParent.findAllMatchIn(inStr)
          .map(x => (x.group(0).toString, s"${x.group(1) + x.group(2)}")),
        regx.findAllMatchIn(inStr)
          .map(x => (x.toString, s"${dropDescription('(')(x)}")),
        regxNull.findAllMatchIn(inStr)
          .map(x => (x.toString, "null")),
        regxDecimal.findAllMatchIn(inStr)
          .map(x => (x.toString, s"${dropDescription('(')(x)}")),
        regxItem.findAllMatchIn(inStr)
          .map(x => (x.toString, s"${dropDescription('[')(x)}")))
        .reduce(_ ++ _).toList.sortBy(x => x._1.size).reverse.foldLeft(inStr) { (l, r) => l.replaceAllLiterally(r._1, r._2) }
      runningDates.foldLeft(replaceBasic) { (l, r) => l.replaceAllLiterally(r._1, r._2) }
    }

    def replaceLitStringMod(org: String) =
      Seq(toSubstr, toPadding).foldLeft(org) { (l, r) => r(l) }

    val toSubstr = (target: String) => {
      """(\w*)/(\d*):(\d*)""".r.findAllMatchIn(target).map { r =>
        val name = r.group(1)
        (r.toString, (r.group(2), r.group(3)) match {
          case (pos, "")                   => s"${name}.drop(${pos.toInt - 1})"
          case ("", len) if len.toInt <= 0 => name
          case ("", len)                   => s"${name}.take(${len})"
          case (pos, len)                  => s"${name}.slice(${pos.toInt - 1}, ${pos.toInt + len.toInt - 1})"
        })
      }.foldLeft(target) { (l, r) => l.replaceAllLiterally(r._1, r._2) }
    }

    val toPadding = (target: String) => {
      """(\w*)/pad(\d*)""".r.findAllMatchIn(target).map { r =>
        val name = r.group(1)
        (r.toString, r.group(2) match {
          case len => s"""${name}.padTo(${len}, ' ')"""
        })
      }.foldLeft(target) { (l, r) => l.replaceAllLiterally(r._1, r._2) }
    }
  }

  object column {
    val replaceLitStr = (s: String) => {
      val rslt = Seq(
        regxWithParent.findAllMatchIn(s)
          .map(x => (x.group(0).toString, s"""$$"${x.group(1) + x.group(2)}"""")),
        regx.findAllMatchIn(s)
          .map(x => (x.toString, s"lit(${dropDescription('(')(x)})")),
        regxNull.findAllMatchIn(s)
          .map(x => (x.toString, "lit(null)")),
        regxDecimal.findAllMatchIn(s)
          .map(x => (x.toString, s"lit(${dropDescription('(')(x)})")),
        regxItem.findAllMatchIn(s)
          .map(x => (x.toString, s"""$$"${dropDescription('[')(x)}"""")))
        .reduce(_ ++ _).toList.sortBy(x => x._1.size).reverse.foldLeft(s) { (l, r) => l.replaceAllLiterally(r._1, r._2) }
      val rslt2 = runningDates.foldLeft(rslt) { (l, r) => l.replaceAllLiterally(r._1, s"lit(${r._2})") }
      Seq(replaceSignStr, toSubstr, toPadding).foldLeft(rslt2) { (l, r) => r(l) }
    }

    val toSubstr = (target: String) => {
      """(\$"\w*)/(\d*):(\d*)"""".r.findAllMatchIn(target).map { r =>
        val name = r.group(1) + "\""
        (r.toString, (r.group(2), r.group(3)) match {
          case (pos, "")  => s"substring(${name}, ${pos}, 1024)"
          case ("", len)  => s"substring(${name}, 1, ${len})"
          case (pos, len) => s"substring(${name}, ${pos}, ${len})"
        })
      }.foldLeft(target) { (l, r) => l.replaceAllLiterally(r._1, r._2) }
    }

    val toPadding = (target: String) => {
      """(\$"\w*)/pad(\d*)"""".r.findAllMatchIn(target).map { r =>
        val name = r.group(1) + "\""
        (r.toString, r.group(2) match {
          case len => s"""rpad(${name}, ${len}, " ")"""
        })
      }.foldLeft(target) { (l, r) => l.replaceAllLiterally(r._1, r._2) }
    }

    val replaceSignStr = (s: String) =>
      Seq(
        (" = ", " === "),
        (" != ", " !== "))
        .foldLeft(s) { (l, r) => l.replaceAllLiterally(r._1, r._2) }

    def replaceJoinStr(inStr: String) = {
      def toColumn(tag: String) = (s: Match) => {
        val item = dropDescription('[')(s).split('.')(1)
        (s.toString, s"""${tag}("${item}")""")
      }

      Seq(
        regxJoinLeft.findAllMatchIn(inStr).map(toColumn("left")),
        regxJoinRight.findAllMatchIn(inStr).map(toColumn("right")))
        .reduce(_ ++ _).toList.foldLeft(inStr) { (l, r) => l.replaceAllLiterally(r._1, r._2) }
    }

    def replaceLit(inStr: Seq[String]) = inStr.map(replaceLitStr)

    def replaceLitUdfStr(s: String) = {
      Seq(
        regxWithParent.findAllMatchIn(s)
          .map(x => (x.group(0).toString, x.group(1) + x.group(2))),
        regx.findAllMatchIn(s)
          .map(x => (x.toString, s"${dropDescription('(')(x)}")),
        regxNull.findAllMatchIn(s)
          .map(x => (x.toString, "null")),
        regxDecimal.findAllMatchIn(s)
          .map(x => (x.toString, s"${dropDescription('(')(x)}")),
        regxItem.findAllMatchIn(s)
          .map(x => (x.toString, s"""${dropDescription('[')(x)}""")))
        .reduce(_ ++ _).toList.sortBy(x => x._1.size).reverse.foldLeft(s) { (l, r) => l.replaceAllLiterally(r._1, r._2) }
    }

    def replaceLitUdf(inStrs: Seq[String]) = {
      val x = inStrs.map(replaceLitUdfStr)
      Seq(scala.toSubstr, scala.toPadding).foldLeft(x) { (l, r) => l.map(x => r(x)) }
    }
  }

  object db {
    def replaceLitStringMod(org: String) =
      Seq(toSubstr, toPadding).foldLeft(org) { (l, r) => r(l) }

    val toSubstr = (target: String) => {
      """(\w*)/(\d*):(\d*)""".r.findAllMatchIn(target).map { r =>
        val name = r.group(1)
        (r.toString, (r.group(2), r.group(3)) match {
          case (pos, "")                   => s"SUBSTR(${name},${pos})"
          case ("", len) if len.toInt <= 0 => name
          case ("", len)                   => s"SUBSTR(${name}, 1, ${len})"
          case (pos, len)                  => s"SUBSTR(${name}, ${pos}, ${len})"
        })
      }.foldLeft(target) { (l, r) => l.replaceAllLiterally(r._1, r._2) }
    }

    val toPadding = (target: String) => {
      """(\w*)/pad(\d*)""".r.findAllMatchIn(target).map { r =>
        val name = r.group(1)
        (r.toString, r.group(2) match {
          case len => s"""RPAD(${name}, ${len})"""
        })
      }.foldLeft(target) { (l, r) => l.replaceAllLiterally(r._1, r._2) }
    }
  }

  object join {
    def genJoinSelectorAppend(str: String, isAllLast: Boolean) = genJoinSelector(str) {
      case ((logic, comment), isLast) =>
        val comma = (isLast, isAllLast) match {
          case (true, true) => ""
          case _            => ","
        }
        s"""("${logic}")${comma} //${comment}"""
    }

    def genJoinSelectorDrop(str: String, isAllLast: Boolean) = genJoinSelector(str) {
      case ((logic, comment), isLast) =>
        val aster = (isLast, isAllLast) match {
          case (true, true)  => """("*")"""
          case (true, false) => """("*"),"""
          case _             => ""
        }
        s""".drop("${logic}")${aster} //${comment}"""
    }

    val regx = """(\S+)\[(.*?)\]""".r
    def genJoinSelector(str: String)(func: ((String, String), Boolean) => String) = {
      val parsed = str.split(',').flatMap {
        case "全て" => Seq(("*", "全て"))
        case s    => regx.findAllMatchIn(s).map(reg => (reg.group(1), reg.group(2)))
      }
      parsed.dropRight(1).map(x => func(x, false)) ++ parsed.takeRight(1).map(x => func(x, true))
    }
  }
}