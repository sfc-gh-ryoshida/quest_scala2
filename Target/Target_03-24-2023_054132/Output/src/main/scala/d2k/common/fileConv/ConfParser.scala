package d2k.common.fileConv

import scala.reflect.io.Path
import scala.reflect.io.Directory
import scala.io.Source
import scala.reflect.io.Path.string2path
import scala.reflect.io.File
import scala.io.BufferedSource
import java.io.FileNotFoundException
import scala.util.Try
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class Conf (appConf: AppConf, itemConfs: Iterator[ItemConf])
case class AppConf (AppId: String, AppName: String, destSystem: String, fileFormat: String, newline: Boolean, header: Boolean, footer: Boolean, storeType: String, inputDir: String, inputFiles: String, comment: String = "")
case class ItemConf (itemId: String, itemName: String, length: String, cnvType: String, extractTarget: Boolean, comment: String = "")
 object ConfParser {
   val availableBoolean = Seq("true", "false", "TRUE", "FALSE")

   val availableBooleanWithBlank = Seq("true", "false", "TRUE", "FALSE", "")

   val availableFileFormat = Seq("tsv", "tsvDropDoubleQuote", "tsvStrict", "csv", "csvStrict", "vsv", "vsvStrict", "fixed")

   val availableStoreType = Seq("all", "diff", "diffExt")

   def parse(inFilePath: String) = parseAppConf(inFilePath)

   def readConf [A](inFilePath: String)(proc: Array[String] => A) = {
   val fileEnc = "MS932"
 val itemConfPath = s"itemConf/${ File(inFilePath).name }"
Option(getClass.getClassLoader.getResourceAsStream(itemConfPath)).map( is =>Source.fromInputStream(is, fileEnc)).getOrElse{
Source.fromFile(inFilePath, fileEnc)
}.getLines.drop(1).map( line =>proc(line.split('\t')))
   }

   def checkItem(items: Array[String])(abailables: Seq[String], targetIdx: Int, comment: String) = if (!abailables.contains(items(targetIdx)))
      {
      throw new IllegalArgumentException (
s"not available item:${ items(targetIdx) }(usage: ${ abailables.mkString(" or ") }) in $comment")
      }

   def parseAppConf(inFilePath: String) = {
   val appConfs = readConf(inFilePath){ items =>appErrorCheck(items)
if (items.size == 10)
         {
         AppConf(items(0), items(1), items(2), items(3), items(4) == "true", items(5) == "true", items(6) == "true", items(7), items(8), items(9))
         }
else
         {
         AppConf(items(0), items(1), items(2), items(3), items(4) == "true", items(5) == "true", items(6) == "true", items(7), items(8), items(9), items(10))
         }
}
 val basePath = Path(inFilePath).toAbsolute.parent
 val namePrefix = Path(inFilePath).name.split('_')(0)
appConfs.map( appConf =>Conf(appConf, parseItemConf(basePath, namePrefix, appConf.AppId)))
   }

   def getAvailableBoolean(items: Array[String]) = {
   if (items(3) == "fixed")
         {
         availableBoolean
         }
else
         {
         availableBooleanWithBlank
         }
   }

   def appErrorCheck(items: Array[String]) = {
   val checker = checkItem(items)_
 def comment(name: String) = s"AppConf[appId:${ items(0) } name:${ name }]"
checker(availableFileFormat, 3, comment("fileFormat"))
checker(getAvailableBoolean(items), 4, comment("newline"))
checker(availableBoolean, 5, comment("header"))
checker(availableBoolean, 6, comment("footer"))
checker(availableStoreType, 7, comment("storeType"))
   }

   def parseItemConf(basePath: Directory, prefix: String, appId: String) = {
   val itemConfPath = basePath / s"${ prefix }_items_${ appId }.conf"
readConf(itemConfPath.toString){ items =>itemErrorCheck(items, appId)
if (items.size == 5)
         {
         ItemConf(items(0), items(1), items(2), items(3), items(4).toLowerCase == "true")
         }
else
         {
         ItemConf(items(0), items(1), items(2), items(3), items(4).toLowerCase == "true", items(5))
         }
}
   }

   def itemErrorCheck(items: Array[String], appId: String) = {
   val checker = checkItem(items)_
 def comment(name: String) = s"ItemConf[appId:${ appId } item:${ items(0) } name:${ name }]"
checkItem(items)(availableBoolean, 4, comment("extractTarget"))
   }
}