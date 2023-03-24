package d2k.appdefdoc.parser

import scala.util.parsing.combinator.JavaTokenParsers
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class ItemDef (id: String, dataType: String, name: String, details: List[ItemDetail])
case class ItemDetail (id: String, name: String, dataType: String, size: String, format: String)
 object TableItem5 {
   def apply(s: String) = {
   val splitted = s.split('|')
new TableItem5 (splitted(1), splitted(2), splitted(3), splitted(4), splitted(5))
   }
}
case class TableItem5 (item1: String, item2: String, item3: String, item4: String, item5: String)
 object ItemDefParser extends JavaTokenParsers with D2kParser {
   def apply(baseUrl: String, branch: String, filePath: String) = {
   val parsed = parseAll(itemDef, readItemDefMd(baseUrl, branch, filePath))
println(parsed)
parsed
   }

   def apply(basePath: String) = {
   parseAll(itemDef, readItemDefMd(basePath))
   }

   val eol = '\n'

   val num2 = "[0-9][0-9]".r

   val anyWords = ".*".r

   val anyWords2 = "^(?!\\|).*".r

   val tableValue5 = anyWords <~ eol ^^ {
   case s => TableItem5(s)
   }

   def itemDef = dataType ~ (itemInfo <~ opt(comment)) ~ itemDetail <~ ".*".r ^^ {
   case a~ b~ c => val details = c.map{ x =>ItemDetail(x.item1.trim, x.item2.trim, x.item3.trim, x.item4.trim, x.item5.trim)
}
 val splitName = b.split("_")
ItemDef(splitName(0), a, splitName(1), details)
   }

   def dataType = "#" ~> "[\\w\\(\\)]*".r <~ "項目定義"

   def itemInfo = "##" ~> anyWords

   def comment = rep(anyWords2)

   def itemDetail = tableTitle1 ~> tableTitle2 ~> rep(tableValue5)

   def tableTitle1 = "|" ~ "物理名" ~ "|" ~ "論理名" ~ "|" ~ ("型(ドメイン)" | "型") ~ "|" ~ "桁数" ~ "|" ~ "フォーマット" ~ "|"

   val tableTitle2 = repN(5, "\\|[\\s\\:\\-]\\-+".r) ~ "|"
}