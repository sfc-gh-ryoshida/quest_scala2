package d2k.appdefdoc.gen.sql

import scala.util.parsing.combinator.JavaTokenParsers
import d2k.appdefdoc.parser.D2kParser
import d2k.appdefdoc.parser.IoData
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object SqlLogicParser extends JavaTokenParsers with D2kParser {
   def apply(baseUrl: String, branch: String, appGroup: String, appId: String) = {
   (parser_ andThen replaceComment_)(readAppDefMd(baseUrl, branch, appGroup, appId, "README.md"))
   }

   def apply(baseUrl: String, appGroup: String, appId: String) = {
   (parser_ andThen replaceComment_)(readAppDefMd(baseUrl, appGroup, appId, "README.md"))
   }

   def parser(s: String) = s.split("```sql")(1).split("```")(0)

   val regxItem = """(\S+)(\[.*?\])""".r

   val regxChar = """([\'].*?[\'])\((.*?)\)""".r

   val regxDecimal = """(\d+)(\(.*?\))""".r

   val regxParent = """(\S+)\[[^\[\]]*\]\.""".r

   def replaceComment(inStr: String) = {
   Seq(
regxChar.findAllMatchIn(inStr).map( x =>(x.toString, x.group(1))), 
regxItem.findAllMatchIn(inStr).map( x =>(x.toString, x.group(1))), 
regxDecimal.findAllMatchIn(inStr).map( x =>(x.toString, x.group(1))), 
regxParent.findAllMatchIn(inStr).map( x =>(x.toString, s"""${ x.group(1) }."""))).reduce(_ ++ _).toList.sortBy( x =>x._1.size).reverse.foldLeft(inStr){(l, r) =>l.replaceAllLiterally(r._1, r._2)}
   }
}