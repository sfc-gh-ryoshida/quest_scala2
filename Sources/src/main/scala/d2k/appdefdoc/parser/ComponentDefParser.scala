package d2k.appdefdoc.parser

import scala.util.parsing.combinator.JavaTokenParsers
import d2k.appdefdoc.gen.src.ItemReplace
import ItemReplace.column._
import scala.util.matching.Regex

case class ComponentDef(
  componentInfo: ComponentInfo, componentDetail: ComponentDetail, implimentLogic: ImplimentLogic)
case class ComponentInfo(id: String, name: String, desc: String)
case class ComponentDetail(componentId: String, processes: List[Process]) {
  def findById(id: String) = processes.map(_.findById(id)).head
  def findById(id: String, subId: String) = processes.map(_.findById(id, subId)).head
}

case class Process(desc: ProcessDesc, detail: List[ProcessDetail]) {
  def findById(id: String) = detail.find(_.param.id == id).get.values.head.asInstanceOf[ProcessParamValue]
  def findById(id: String, subId: String) = detail.find(_.param.id == id).flatMap(_.findById(subId)).get.asInstanceOf[ProcessParamSubDetail]
}
case class ProcessDesc(id: String, value: String)
case class ProcessDetail(param: ProcessParam, defaultCode: Option[ProcessDefaultCode], values: List[ProcessParamBase]) {
  def findById(id: String) = values.find(_.id == id)
}

sealed trait ProcessParamBase {
  val id: String
}
case class ProcessParam(id: String, name: String)
case class ProcessParamValue(value: String) extends ProcessParamBase { val id = "" }
case class ProcessParamSubDetail(id: String, name: String, defaultCode: ProcessDefaultCode, value: Seq[String]) extends ProcessParamBase
case class ProcessDefaultCode(value: String)

case class ImplimentLogic(defaultCode: ImplimentDefaultCode, impls: List[ImplimentLogicBase])
case class ImplimentDefaultCode(value: String)
case class ImplimentValue(value: String)

object TableItem2 {
  def apply(s: String) = {
    val splitted = s.split('|')
    new TableItem2(splitted(1), splitted(2))
  }
}
case class TableItem2(item1: String, item2: String)

object TableItem3 {
  def apply(s: String) = {
    val splitted = s.split('|')
    new TableItem3(splitted(1), splitted(2), splitted(3))
  }
}
case class TableItem3(item1: String, item2: String, item3: String)

object CastTableItem {
  def apply(s: String) = {
    val splitted = s.drop(1).dropRight(1).split("\\s+\\|\\s+")
    new CastTableItem(splitted(0).trim, splitted(1).trim)
  }
}
case class CastTableItem(item1: String, item2: String)

sealed trait ImplimentLogicBase {
  val id: String
  def genSrc(tmpl: String): String
  def addDq(s: String) = {
    val trimed = s.trim
    if (trimed.startsWith("\"")) trimed else s""""${trimed}""""
  }

  def removeComments(s: String) = replaceLit(Seq(s)).head

  def tbl2ToStr(tbls: Seq[TableItem2], doAddDq: Boolean = true) = {
    val bottomData = tbls.last
    (tbls.dropRight(1).map { s =>
      s"      ${if (doAddDq) addDq(s.item1) else s.item1}, //${s.item2}"
    } :+ {
      s"      ${if (doAddDq) addDq(bottomData.item1) else bottomData.item1}  //${bottomData.item2}"
    }).mkString(" \n")
  }
}

case class Impliment全体編集(id: String, desc: String, editors: Seq[ImplimentEditor]) extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
      .replaceAllLiterally("%%funcs%%", editors.map(x => x.genStr).mkString(",\n"))
  }
}

case class Impliment部分編集(id: String, desc: String, editors: Seq[ImplimentEditor]) extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
      .replaceAllLiterally("%%funcs%%", editors.map(x => x.genStr).mkString(",\n"))
  }
}

sealed trait ImplimentEditor {
  def genStr: String
  def addDq(s: String) = {
    val trimed = s.trim
    if (trimed.startsWith("\"")) trimed else s""""${trimed}""""
  }

  val cnvMap = Seq(
    (" =\\s*", " == "),
    //    (" _\\s*", "_"),
    ("and\\s*", "&& "),
    ("or\\s*", "|| "),
    ("if\\s*", "if( "),
    ("else\\s*$", "} else { "),
    ("elseif\\s*", "} else if "),
    ("elsif\\s*", "} else if "),
    ("elif\\s*", "} else if "),
    ("then\\s*", "){ "),
    ("end\\s*", "} "))
  def cnvToScala(logic: Seq[String]): Seq[String] =
    logic.map { cnvMap.foldLeft(_) { case (l, (org, dest)) => l.replaceAll(org, dest) } }

  val reg = "(\\w+_)*\\w{2}_\\w+".r
  def varList(target: Seq[String]) = reg.findAllMatchIn(target.mkString).map(_.matched).toSet.toSeq

  def mkCallArgs(target: Seq[String]) = {
    val arg = target.map(x => s"""$$"${x}"""").mkString(", ")
    if (target.size <= 10) {
      arg
    } else {
      s"array(${arg})"
    }
  }
  def varType(s: String) = {
    val splitted = s.split('_')
    val prefixType = if (splitted.size >= 2) {
      splitted.dropRight(1).takeRight(1)
    } else {
      splitted
    }

    prefixType.headOption.map {
      case "NM" | "AM" => "jBigDecimal"
      case _           => "String"
    }.getOrElse("String")
  }
  def mkArgs(target: Seq[String]) =
    if (target.size <= 10) {
      target.map { x =>
        s"${x}: ${varType(x)}"
      }.mkString(", ")
    } else {
      "arr:Seq[Any]"
    }
  def argMoveList(target: Seq[String]) =
    if (target.size <= 10) {
      ""
    } else {
      target.zipWithIndex.map {
        case (item, idx) =>
          s"val ${item} = arr(${idx}).asInstanceOf[${varType(item)}]"
      }.mkString("\n          ", "\n          ", "")
    }
}

case class ImplimentEdit(no: String, id: String, name: String, logic: Seq[String]) extends ImplimentEditor {
  def genStr = {
    val convedLogic = cnvToScala(replaceLitUdf(logic))
    val vl = varList(convedLogic)
    if (logic.size == 1) {
      val repLogic = if (logic(0).trim == "編集無し") Seq(s"""$$"${id}"""") else logic
      s"""\n      //${no} ${name.replaceAllLiterally("[\\[\\]]", "")}\n      (${addDq(id)}, ${replaceLit(repLogic).mkString("\n")}).e"""
    } else {
      s"""
      //${no} ${name.replaceAllLiterally("[\\[\\]]", "")}
        \\      {
        \\        val func = udf{(${mkArgs(vl)}) =>${argMoveList(vl)}
        \\        ${ItemReplace.runningDates.foldLeft(convedLogic.mkString("\n          ")) { (l, r) => l.replaceAllLiterally(r._1, r._2) }}
        \\        }
        \\        (${addDq(id)}, func(${mkCallArgs(vl)}))
        \\      }.e""".stripMargin('\\')
    }
  }
}

case class ImplimentRename(no: String, srcId: String, srcName: String, destId: String, destName: String, logic: Seq[String]) extends ImplimentEditor {
  def genStr = {
    val convedLogic = cnvToScala(replaceLitUdf(logic))
    val vl = varList(convedLogic)
    if (logic.isEmpty)
      s"\n      //${no} ${srcName} -> ${destName}\n      (${addDq(srcId)} -> ${addDq(destId)}).r"
    else {
      if (logic.size == 1) {
        s"\n      //${no} ${srcName} -> ${destName}\n      (${addDq(srcId)} -> ${addDq(destId)}, ${replaceLit(logic).mkString("\n")}).r"
      } else {
        s"""
        \\      //${no} ${srcName} -> ${destName}      
        \\      {
        \\        val func = udf{(${mkArgs(vl)}) =>${argMoveList(vl)}
        \\        ${convedLogic.mkString("\n          ")}
        \\        }
        \\        (${addDq(srcId)} -> ${addDq(destId)}, func(${mkCallArgs(vl)}))
        \\      }.r""".stripMargin('\\')
      }

    }
  }
}

case class ImplimentDelete(no: String, id: String, name: String) extends ImplimentEditor {
  def genStr = s"\n      //${no} ${name} -> delete\n      ${addDq(id)}.d"
}

case class Impliment選択(id: String, desc: String, data: Seq[TableItem2]) extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
      .replaceAllLiterally("%%funcs%%", tbl2ToStr(data))
  }
}

case class Impliment抽出(id: String, desc: String, cond: Seq[String]) extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
      .replaceAllLiterally("%%funcs%%", ItemReplace.column.replaceLit(cond).mkString("\n"))
  }
}

case class Implimentキャスト(id: String, desc: String, castType: String, data: Seq[TableItem2]) extends ImplimentLogicBase {
  def postValue(s: String) = if (s.trim.startsWith("[正規表現]")) "cr" else "c"
  def replacePipe(s: String) = s.replaceAllLiterally("&#124;", "|")
  def tbl2ToStr(tbls: Seq[TableItem2]) = {
    val bottomTi = tbls.last
    (tbls.dropRight(1).map { ti =>
      s"      (${addDq(replacePipe(ti.item1))}, ${addDq(castType)}).${postValue(ti.item2)}, //${ti.item2}"
    } :+ {
      s"      (${addDq(replacePipe(bottomTi.item1))}, ${addDq(castType)}).${postValue(bottomTi.item2)} //${bottomTi.item2}"
    }).mkString(" \n")
  }

  def genSrc(tmpl: String) = {
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
      .replaceAllLiterally("%%funcs%%", tbl2ToStr(data))
  }
}

case class Impliment出力項目並び替え(id: String, tbl: TableItem2, baseUrl: String = "", branch: String = "") extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    val item = tbl.item1.trim.split("\\]\\(")
    val resId = item(0).tail
    val filePath = item(1).dropRight(1).split("/").toList.takeRight(3).mkString("/")
    println(s"  Parsing ${resId}[${tbl.item2.trim}]")
    val itemPs = ItemDefParser(baseUrl, branch, filePath)
    val items = tbl2ToStr(itemPs.get.details.map(x => TableItem2(x.id, x.name)))
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", s"出力項目並び替え ${resId}[${tbl.item2.trim}]")
      .replaceAllLiterally("%%items%%", items)
  }
}

case class Implimentグループ抽出(id: String, desc: String, grpKeys: Seq[TableItem2], sortKeys: Seq[TableItem3]) extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    val sortKeyItem2 = sortKeys.map { t =>
      val sortParam = if (t.item3.trim.startsWith("降順")) "desc" else "asc"
      TableItem2(s"$$${addDq(t.item1)}.${sortParam}", s"${t.item2.trim} ${t.item3.trim}")
    }
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
      .replaceAllLiterally("%%grpKeys%%", tbl2ToStr(grpKeys))
      .replaceAllLiterally("%%sortKeys%%", tbl2ToStr(sortKeyItem2, false))
  }
}

case class Impliment集計(id: String, desc: String, grpKeys: Seq[TableItem2], aggKeys: Seq[TableItem3]) extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    val aggkeyItem2 = aggKeys.map { t =>
      val sortParam = if (t.item3.trim.startsWith("降順")) "desc" else "asc"
      val item1Dq = addDq(t.item1)
      TableItem2(s"${t.item3.trim.toLowerCase}(${item1Dq}) as ${item1Dq}", s"${t.item2.trim}")
    }
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
      .replaceAllLiterally("%%grpKeys%%", tbl2ToStr(grpKeys))
      .replaceAllLiterally("%%aggKeys%%", tbl2ToStr(aggkeyItem2, false))
  }
}

case class Impliment再分割(id: String, desc: String, size: Int) extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
      .replaceAllLiterally("%%size%%", size.toString)
  }
}

case class Implimentキャッシュ(id: String, desc: String) extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
  }
}

case class Implimentキャッシュ解放(id: String, desc: String) extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
  }
}

case class Impliment関数定義(id: String, desc: String, args: Seq[TableItem3], impls: Seq[String]) extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    val argsItem2 = args.map { t =>
      TableItem2(s"${t.item1.trim}: ${t.item3.trim}", s"${t.item2.trim}")
    }
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
      .replaceAllLiterally("%%args%%", tbl2ToStr(argsItem2, false))
      .replaceAllLiterally("%%impls%%", impls.map(x => s"        $x").mkString("\n"))
  }
}

case class Impliment関数呼出(id: String, desc: String, funcName: String, args: Seq[TableItem3]) extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    val funcItem2 = args.map { t =>
      TableItem2(s"    ${t.item1.trim}", s"${t.item2.trim}")
    }
    s"\n        ${funcName}(\n${tbl2ToStr(funcItem2, false)})"
  }
}

case class ImplimentUDF関数定義(id: String, desc: String, args: Seq[TableItem3], impls: Seq[String]) extends ImplimentLogicBase {
  def genSrc(tmpl: String) = {
    val argsItem2 = args.map { t =>
      TableItem2(s"${t.item1.trim}: ${t.item3.trim}", s"${t.item2.trim}")
    }
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
      .replaceAllLiterally("%%args%%", tbl2ToStr(argsItem2, false))
      .replaceAllLiterally("%%impls%%", impls.map(x => s"        $x").mkString("\n"))
  }
}

case class ImplimentUDF関数適用(id: String, desc: String, applyLogic: String, data: Seq[TableItem2]) extends ImplimentLogicBase {
  def postValue(s: String) = if (s.trim.startsWith("[正規表現]")) "ar" else "a"
  def replacePipe(s: String) = s.replaceAllLiterally("&#124;", "|")
  def tbl2ToStr(tbls: Seq[TableItem2]) = {
    val bottomTi = tbls.last
    (tbls.dropRight(1).map { ti =>
      s"      (${addDq(replacePipe(ti.item1))}, (col: Column) => ${applyLogic}).${postValue(ti.item2)}, //${ti.item2}"
    } :+ {
      s"      (${addDq(replacePipe(bottomTi.item1))}, (col: Column) => ${applyLogic}).${postValue(bottomTi.item2)} //${bottomTi.item2}"
    }).mkString(" \n")
  }

  def genSrc(tmpl: String) = {
    tmpl
      .replaceAllLiterally("%%id%%", id)
      .replaceAllLiterally("%%desc%%", desc)
      .replaceAllLiterally("%%funcs%%", tbl2ToStr(data))
  }
}

object ComponentDefParser extends JavaTokenParsers with D2kParser {
  def apply(baseUrl: String, branch: String, appGroup: String, appId: String, fileName: String) = {
    val parsed = parseAll(componentDef, readAppDefMd(baseUrl, branch, appGroup, appId, fileName))
    println(parsed)
    parsed
  }

  def apply(baseUrl: String, appGroup: String, appId: String, fileName: String) = {
    val parsed = parseAll(componentDef, readAppDefMd(baseUrl, appGroup, appId, fileName))
    println(parsed)
    parsed
  }

  val eol = '\n'
  val num2 = "[0-9][0-9]".r
  val anyWords = "^(?!(#{5}|#{4}|#{3}|#{2}|-{4})).*".r
  val tableValue2 = anyWords <~ eol ^^ { case s => TableItem2(s) }
  val tableValue3 = anyWords <~ eol ^^ { case s => TableItem3(s) }

  def componentDef = title ~> componentInfo ~ componentDetail ~ implimentLogic ~ ".*".r ^^ {
    case a ~ b ~ c ~ _ => ComponentDef(a, b, c)
  }

  def title = "#" ~> "コンポーネント定義"
  def componentInfo = "##" ~> componentIdAndName ~ ".*".r ~ componentDesc ^^ {
    case a ~ b ~ c => ComponentInfo(s"${a._1}_${a._2}", b, c)
  }
  def componentIdAndName = componentDefId ~ componentName ^^ { case a ~ b => (a, b) }
  def componentDefId = num2 <~ "_"
  def componentName = "\\w*".r
  def componentDesc = "###" ~> "処理概要" ~> ".*".r

  def componentDetail = componentId ~ (opt("----") ~> rep(process)) ^^ { case a ~ b => ComponentDetail(a, b) }
  def componentId = "----" ~> "###" ~> "コンポーネントID" ~> "#####" ~> ".*".r ~> ident

  def process = processDesc ~ rep(processDetail1) ^^ { case a ~ b => Process(a, b) }
  def processDesc = ("##" ~> "[" ~> ident) ~ ("]" ~> ".*".r) ^^ { case a ~ b => ProcessDesc(a, b) }
  def processDetail1 = processParam ~ opt(processDefaultCode) ~ rep(processParamSubDetail | processValue) ^^ {
    case a ~ b ~ c => ProcessDetail(a, b, c)
  }
  def processParam = "###" ~> (num2 <~ ".") ~ anyWords ^^ { case a ~ b => ProcessParam(a, b) }
  def processDefaultCode = "#####" ~> anyWords ^^ { case a => ProcessDefaultCode(a) }
  def processValue = anyWords ^^ { case a => ProcessParamValue(a) }
  def processParamSubDetail = ("####" ~> "[0-9][0-9]\\.[0-9][0-9]".r <~ ".") ~ anyWords ~ processDefaultCode ~ rep(anyWords) ^^ {
    case a ~ b ~ c ~ d => ProcessParamSubDetail(a, b, c, d)
  }

  def implimentLogic = (implimentTitle ~> implimentDefaultCode) ~ (opt(implimentFlowDiagram) ~> impliment) ^^ { case a ~ b => ImplimentLogic(a, b) }
  def implimentTitle = "----" ~ "##" ~ "実装ロジック"
  def implimentDefaultCode = "#####" ~> ".*".r ^^ { case a => ImplimentDefaultCode(a) }
  def implimentFlowDiagram = "###" ~ "実装ロジックフロー図" ~ "```plantuml" ~ rep(implimentFlowDiagramDetail) ~ "```"
  def implimentFlowDiagramDetail = "[\\w\\(\\)\\*]*".r ~> "->" ~> ".*".r

  def impliment = rep(
    impliment全体編集 | impliment部分編集 | impliment選択 | impliment抽出 | implimentキャスト |
      impliment出力項目並び替え | implimentグループ抽出 | impliment集計 |
      impliment再分割 | implimentキャッシュ解放 | implimentキャッシュ |
      impliment関数定義 | impliment関数呼出 | implimentUDF関数定義 | implimentUDF関数適用)

  val implimentDetailId = "###" ~> "\\w{3}".r <~ "."
  val implimentDesc = anyWords
  val implimentValue = anyWords <~ eol

  def tableHeader2 = tableHeader2_1 ~ tableHeader2_2
  val tableHeader2_1 = "|" ~ "物理名" ~ "|" ~ "論理名" ~ "|"
  val tableHeader2_2 = repN(2, "\\|[\\s\\:\\-]\\-+".r) ~ "|"

  def tableHeader3(target: String) = tableHeader3_1(target) ~ tableHeader3_2
  def tableHeader3_1(target: String) = "|" ~ "物理名" ~ "|" ~ "論理名" ~ "|" ~ target ~ "|"
  val tableHeader3_2 = repN(3, "\\|[\\s\\:\\-]\\-+".r) ~ "|"

  def impliment全体編集 = implimentDetailId ~ ("全体編集" ~> anyWords) ~ rep(implimentDelete | implimentRename | implimentEdit) ^^ {
    case a ~ b ~ c => Impliment全体編集(a, b, c)
  }

  def impliment部分編集 = implimentDetailId ~ ("部分編集" ~> anyWords) ~ rep(implimentDelete | implimentRename | implimentEdit) ^^ {
    case a ~ b ~ c => Impliment部分編集(a, b, c)
  }

  def implimentEdit = ("####" ~> "\\d{3}".r) ~ (":" ~> ident) ~ "\\[.*\\]".r ~ rep(implimentValue) ^^ {
    case a ~ b ~ c ~ d => ImplimentEdit(a, b, c, d)
  }
  def implimentRename = ("####" ~> "\\d{3}".r) ~ (":" ~> ident) ~ "\\[(.*?)\\]".r ~ ("->" ~> ident) ~ "\\[(.*?)\\]".r ~ rep(implimentValue) ^^ {
    case a ~ b ~ c ~ d ~ e ~ f => ImplimentRename(a, b, c, d, e, f)
  }
  def implimentDelete = ("####" ~> "\\d{3}".r) ~ (":" ~> ident) ~ ("\\[.*\\]".r <~ "->" <~ "delete") ^^ {
    case a ~ b ~ c => ImplimentDelete(a, b, c)
  }

  def implimentキャスト = (implimentDetailId <~ "キャスト") ~ implimentDesc ~ implimentCastDetail1 ~ implimentCastDetail2 ^^ {
    case a ~ b ~ c ~ d => Implimentキャスト(a, b, c, d)
  }
  def implimentCastDetail1 = ("####" ~ "01." ~ "キャスト型") ~> anyWords
  def implimentCastDetail2 = ("####" ~ "02." ~ "キャスト対象項目" ~ tableHeader2) ~> rep(tableValue2)

  def impliment選択 = implimentDetailId ~ ("選択" ~> implimentDesc) ~ implimentSelect ^^ {
    case a ~ b ~ c => Impliment選択(a, b, c)
  }
  def implimentSelect = ("####" ~ "01." ~ "選択項目") ~> tableHeader2 ~> rep(tableValue2)

  def impliment抽出 = implimentDetailId ~ ("抽出" ~> implimentDesc) ~ implimentAbstruction ^^ {
    case a ~ b ~ c => Impliment抽出(a, b, c)
  }

  def implimentAbstruction = ("####" ~ "01." ~ "抽出条件") ~> rep(implimentDesc <~ eol)

  def impliment出力項目並び替え = (implimentDetailId <~ "出力項目並び替え") ~
    (implimentOutItemTable2_1 ~> implimentOutItemTable2_2 ~> implimentOutItemTable2) ^^ {
      case a ~ b => Impliment出力項目並び替え(a, b)
    }

  val implimentOutItemTable2_1 = "|" ~ "項目定義物理名" ~ "|" ~ "項目定義論理名" ~ "|"
  val implimentOutItemTable2_2 = "|" ~ "\\-*".r ~ "|" ~ "\\-*".r ~ "|"
  val implimentOutItemTable2 = anyWords ^^ { case s => TableItem2(s) }

  def implimentグループ抽出 = (implimentDetailId <~ "グループ抽出") ~ implimentDesc ~ implimentGrpAbstDetail1 ~ implimentGrpAbstDetail2 ^^ {
    case a ~ b ~ c ~ d => Implimentグループ抽出(a, b, c, d)
  }
  def implimentGrpAbstDetail1 = "####" ~ "01." ~ "グルーピングキー" ~> tableHeader2 ~> rep(tableValue2)
  def implimentGrpAbstDetail2 = "####" ~ "02." ~ "ソートキー" ~> tableHeader3("ソート順") ~> rep(tableValue3)

  def impliment集計 = (implimentDetailId <~ "集計") ~ implimentDesc ~ implimentSumDetail1 ~ implimentSumDetail2 ^^ {
    case a ~ b ~ c ~ d => Impliment集計(a, b, c, d)
  }
  def implimentSumDetail1 = ("####" ~ "01." ~ "グルーピングキー" ~ tableHeader2) ~> rep(tableValue2)
  def implimentSumDetail2 = ("####" ~ "02." ~ "集計キー" ~ tableHeader3("集計パターン")) ~> rep(tableValue3)

  def impliment再分割 = (implimentDetailId <~ "再分割") ~ implimentDesc ~ implimentRep ^^ {
    case a ~ b ~ c => Impliment再分割(a, b, c.toInt)
  }
  def implimentRep = "####" ~> "01." ~> "再分割数" ~> decimalNumber

  def implimentキャッシュ = (implimentDetailId <~ "キャッシュ") ~ implimentDesc ^^ {
    case a ~ b => Implimentキャッシュ(a, b)
  }

  def implimentキャッシュ解放 = (implimentDetailId <~ "キャッシュ解放") ~ implimentDesc ^^ {
    case a ~ b => Implimentキャッシュ解放(a, b)
  }

  def impliment関数定義 = ("###" ~> "\\w*".r) ~ ("." ~> "関数定義" ~> implimentDesc) ~ implimentDefFuncDetail1 ~ implimentDefFuncDetail2 ^^ {
    case a ~ b ~ c ~ d => Impliment関数定義(a, b, c, d)
  }
  def implimentDefFuncDetail1 = "####" ~ "01." ~ "入力シグネチャ" ~> tableHeader3("型") ~> rep(tableValue3)
  val implimentDefFuncDetail2 = "####" ~ "02." ~ "実装内容" ~> rep(implimentValue)

  def impliment関数呼出 = ("###" ~> "\\w*".r) ~ ("." ~> "関数呼出" ~> implimentDesc) ~ implimentCallFuncDetail1 ~ implimentCallFuncDetail2 ^^ {
    case a ~ b ~ c ~ d => Impliment関数呼出(a, b, c, d)
  }
  val implimentCallFuncDetail1 = "####" ~ "01." ~ "関数名" ~> implimentDesc
  def implimentCallFuncDetail2 = "####" ~ "02." ~ "呼出パラメータ" ~> tableHeader3("型") ~> rep(tableValue3)

  def implimentUDF関数定義 = ("###" ~> "\\w*".r) ~ ("." ~> "UDF関数定義" ~> implimentDesc) ~ implimentDefUdfFuncDetail1 ~ implimentDefUdfFuncDetail2 ^^ {
    case a ~ b ~ c ~ d => ImplimentUDF関数定義(a, b, c, d)
  }
  def implimentDefUdfFuncDetail1 = "####" ~ "01." ~ "入力シグネチャ" ~> tableHeader3("型") ~> rep(tableValue3)
  val implimentDefUdfFuncDetail2 = "####" ~ "02." ~ "実装内容" ~> rep(implimentValue)

  def implimentUDF関数適用 = (implimentDetailId <~ "UDF関数適用") ~ implimentDesc ~ implimentApplyDetail1 ~ implimentApplyDetail2 ^^ {
    case a ~ b ~ c ~ d => ImplimentUDF関数適用(a, b, c, d)
  }
  def implimentApplyDetail1 = ("####" ~ "01." ~ "適用関数") ~> anyWords
  def implimentApplyDetail2 = ("####" ~ "02." ~ "適用対象項目" ~ tableHeader2) ~> rep(tableValue2)
}
