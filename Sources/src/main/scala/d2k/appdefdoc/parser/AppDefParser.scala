package d2k.appdefdoc.parser

import scala.util.parsing.combinator.JavaTokenParsers

case class AppDef(
  appInfo: AppInfo, componentFlow: Option[CfData], componentList: Seq[ComponentDefInfo], inputList: Seq[IoData], outputList: Seq[IoData])
case class AppInfo(id: String, name: String, desc: String)
case class ComponentDefInfo(id: String, mdName: String, name: String)
case class IoData(id: String, path: String, srcType: String, name: String)
object AppDefParser extends JavaTokenParsers with D2kParser {

  def apply(baseUrl: String, branch: String, appGroup: String, appId: String) = {
    val parsed = parseAll(appDef, readAppDefMd(baseUrl, branch, appGroup, appId, "README.md"))
    println(parsed)
    parsed
  }

  def apply(baseUrl: String, appGroup: String, appId: String) = {
    val parsed = parseAll(appDef, readAppDefMd(baseUrl, appGroup, appId, "README.md"))
    println(parsed)
    parsed
  }

  def apply(path: String) = {
    parseAll(appDef, readAppDefMd(path))
  }

  def eol = '\n'
  def separator = "----" ~ eol
  val tableSep2 = repN(2, "\\|[\\s\\:\\-]\\-+".r) ~ "|"
  val tableSep3 = repN(3, "\\|[\\s\\:\\-]\\-+".r) ~ "|"
  val anyWords = "^(?!(#{5}|#{4}|#{3}|#{2}|-{4})).*".r

  def appDef = アプリ情報 ~ コンポーネント情報 ~ 入力データ情報 ~ 出力データ情報 ^^ {
    case a ~ b ~ c ~ d => AppDef(a, Option(ComponentFlowParser(c._1.mkString("\n")).getOrElse(null)), b, c._2, d)
  }

  def アプリケーション定義 = "#" ~> "アプリケーション定義" ~ eol
  def アプリId = "##" ~> ident <~ eol
  def アプリ名 = ".*".r <~ eol
  def 概要 = "##" ~> "概要" ~ eol ~> anyWords <~ eol
  def アプリ情報 = アプリケーション定義 ~> アプリId ~ アプリ名 ~ 概要 <~ separator ^^ {
    case a ~ b ~ c => AppInfo(a, b, c)
  }

  def コンポーネント情報 = コンポーネントタイトル ~> repsep(コンポーネントリスト, eol)

  def コンポーネントタイトル = コンポーネントタイトル1 ~> コンポーネントタイトル2 ~> tableSep2
  def コンポーネントタイトル1 = "##" ~> "01. コンポーネント一覧" ~ eol
  def コンポーネントタイトル2 = "|" ~ "コンポーネントID" ~ "|" ~ "コンポーネント名" ~ "|" ~ eol

  def コンポーネントリスト = コンポーネントリストLeft ~ コンポーネントリストRight ^^ {
    case a ~ b => ComponentDefInfo(a._1, a._2, b.trim)
  }
  def コンポーネントリストLeft = "|" ~> コンポーネントId <~ "|"
  def コンポーネントリストRight = "[^|]+".r <~ "|"

  def コンポーネントId = コンポーネントName ~ コンポーネントmdPath
  def コンポーネントName = "[" ~> "\\w*".r <~ "]"
  def コンポーネントmdPath = "(" ~> "[\\w\\.]*".r <~ ")"

  def コンポーネントフロー = (separator ~ "##" ~ "02. コンポーネントフロー図") ~> コンポーネントフロー定義
  def コンポーネントフロー定義 = "```plantuml" ~> repsep("[^`]*".r, eol) <~ ("```" ~ eol ~ separator)

  def IOデータ = IOデータ1 ~ IOデータ2 ~ IOデータ3 ^^ {
    case a ~ b ~ c => IoData(a._1, a._2, b, c.trim)
  }
  def IOデータ1 = "|" ~> IOデータ定義ID ~ IOデータPath
  def IOデータ2 = "|" ~> "[\\w\\(\\)]*".r
  def IOデータ3 = "|" ~> "[^|]*".r <~ "|"
  def IOデータ定義ID = "[" ~> "[\\w\\.]*".r <~ "]"
  def IOデータPath = "(" ~> "[\\w\\./]*".r <~ ")"

  def 入力データタイトル =
    コンポーネントフロー <~ ("##" ~ "03. 入出力データ一覧" ~ "###" ~ "03.01. 入力" ~ "|" ~ "定義ID" ~ "|" ~ "ソースタイプ" ~ "|" ~ "定義名" ~ "|" ~ eol ~ tableSep3)
  def 入力データ情報 = 入力データタイトル ~ rep(IOデータ) ^^ { case a ~ b => (a, b) }

  def 出力データタイトル =
    "###" ~ "03.02. 出力" ~ "|" ~ "定義ID" ~ "|" ~ "ソースタイプ" ~ "|" ~ "定義名" ~ "|" ~ eol ~ tableSep3
  def 出力データ情報 = 出力データタイトル ~> rep(IOデータ)
}
