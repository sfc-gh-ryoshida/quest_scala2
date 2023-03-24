package d2k.appdefdoc.gen.sql

import scala.util.parsing.combinator.JavaTokenParsers
import d2k.appdefdoc.parser.D2kParser
import d2k.appdefdoc.parser.IoData
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class SqlDef (appInfo: SqlInfo, sqlLogic: Seq[String], inputList: Seq[IoData], outputList: Seq[IoData])
case class SqlInfo (id: String, name: String, desc: String)
 object SqlDefParser extends JavaTokenParsers with D2kParser {
   def apply(baseUrl: String, branch: String, appGroup: String, appId: String) = {
   val parsed = parseAll(sqlDef, readAppDefMd(baseUrl, branch, appGroup, appId, "README.md"))
println(parsed)
parsed
   }

   def apply(baseUrl: String, appGroup: String, appId: String) = {
   val parsed = parseAll(sqlDef, readAppDefMd(baseUrl, appGroup, appId, "README.md"))
println(parsed)
parsed
   }

   def eol = '\n'

   def separator = "----" ~ eol

   val tableSep2 = repN(2, "\\|[\\s\\:\\-]\\-+".r) ~ "|"

   val tableSep3 = repN(3, "\\|[\\s\\:\\-]\\-+".r) ~ "|"

   val anyWords = "^(?!(#{5}|#{4}|#{3}|#{2}|-{4})).*".r

   def sqlDef = SQL情報 ~ (SQLロジック <~ リレーション図) ~ 入力データ情報 ~ 出力データ情報 ^^ {
   case a~ b~ c~ d => SqlDef(a, b, c, d)
   }

   def SQL定義タイトル = "#" ~> "SQL定義" ~ eol

   def SQLId = "##" ~> ident <~ eol

   def SQL名 = ".*".r <~ eol

   def 概要 = "##" ~> "概要" ~ eol ~> repsep(anyWords, eol) <~ eol

   def SQL情報 = (SQL定義タイトル ~> SQLId) ~ SQL名 ~ (概要 <~ separator) ^^ {
   case a~ b~ c => SqlInfo(a, b, c.mkString)
   }

   def SQLロジック = {
   "##" ~> "01. SQL" ~> ("```sql" ~> repsep("^(?!(`{3})).*".r, eol) <~ "```")
   }

   def リレーション図 = separator ~ "##" ~> "02. リレーション図" ~> リレーション図定義

   def リレーション図定義 = "```plantuml" ~ repsep("[^`]*".r, eol) ~ "```" ~ eol ~ separator

   def IOデータ = IOデータ1 ~ IOデータ2 ~ IOデータ3 ^^ {
   case a~ b~ c => IoData(a._1, a._2, b, c.trim)
   }

   def IOデータ1 = "|" ~> IOデータ定義ID ~ IOデータPath

   def IOデータ2 = "|" ~> "[\\w\\(\\)]*".r

   def IOデータ3 = "|" ~> "[^|]*".r <~ "|"

   def IOデータ定義ID = "[" ~> "[\\w\\.]*".r <~ "]"

   def IOデータPath = "(" ~> "[\\w\\./]*".r <~ ")"

   def 入力データタイトル = "##" ~ "03. 入出力データ一覧" ~ "###" ~ "03.01. 入力" ~ "|" ~ "定義ID" ~ "|" ~ "ソースタイプ" ~ "|" ~ "定義名" ~ "|" ~ eol ~ tableSep3

   def 入力データ情報 = 入力データタイトル ~> rep(IOデータ)

   def 出力データタイトル = "###" ~ "03.02. 出力" ~ "|" ~ "定義ID" ~ "|" ~ "ソースタイプ" ~ "|" ~ "定義名" ~ "|" ~ eol ~ tableSep3

   def 出力データ情報 = 出力データタイトル ~> rep(IOデータ)
}