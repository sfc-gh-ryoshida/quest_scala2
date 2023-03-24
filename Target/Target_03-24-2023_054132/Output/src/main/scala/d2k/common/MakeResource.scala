package d2k.common

import java.sql.Timestamp
import java.time.LocalDateTime
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.Row
import spark.common.SparkContexts
import d2k.common.fileConv.Converter
import d2k.common.fileConv.ConfParser
import d2k.common.fileConv.ItemConf
import spark.common.FileCtl
import d2k.common.fileConv.DomainProcessor
import scala.io.Source
import SparkContexts.context.implicits._
import com.snowflake.snowpark.types._
import scala.collection.JavaConverters._
import spark.common.PqCtl
import spark.common.DbInfo
import d2k.common.df.DbConnectionInfo
import spark.common.DbCtl
import com.snowflake.snowpark.SaveMode
import scala.util.Try
import org.scalatest.MustMatchers._
import java.io.FileNotFoundException
import com.snowflake.snowpark.Column
import java.sql.Date
import d2k.common.JefConverter.implicits.JefConvert
import java.io.FileOutputStream
import scala.io.BufferedSource
import org.slf4j.LoggerFactory
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class TableInfo (itemName: String, itemId: String, length: String, cnvType: String, data: String, maxLen: Int)
 object MakeResource {
   val logger = LoggerFactory.getLogger(this.getClass)

   def apply = new MakeResource ("")

   def makeBdField(len: String, name: String) = {
   val (p, s) = if (len.trim.isEmpty)
         {
         (10, 0)
         }
else
         {
         val ps = len.trim.split(',')
if (ps.size == 2)
               {
               (ps(0).toInt, ps(1).toInt)
               }
else
               {
               (len.toInt, 0)
               }
         }
StructField(name, DecimalType(p, s), true)
   }

   def itemConfToTable(confPath: String) = {
   val confs = parseItemConf(confPath)
 val tableInfos = confs.map{ c => val data = DomainProcessor.exec(c.cnvType, "x" * c.length.toInt).right.get.mkString("\"", "", "\"")
 val ml = Seq(toLength(c.itemName), toLength(c.itemId), toLength(c.length), toLength(c.cnvType), toLength(data)).max
 val toMl = toMaxLength(ml)_
TableInfo(toMl(c.itemName), toMl(c.itemId), toMl(c.length), toMl(c.cnvType), toMl(data), ml)
}
tableInfoToTableStr(tableInfos)
   }

   def itemConfMdToTable(url: String) = {
   val md = Source.fromURL(s"${ url }?private_token=${ sys.env("GITLAB_TOKEN") }").getLines.toList.drop(6)
 val tableInfos = md.map{ line => val items = line.split('|').drop(1).map(_.trim)
 val (id, name, domain, length) = (items(0), items(1), items(2), items(3))
 val data = DomainProcessor.exec(domain, "x" * length.toInt).right.get.mkString("\"", "", "\"")
 val ml = Seq(toLength(name), toLength(id), toLength(length), toLength(domain), toLength(data)).max
 val toMl = toMaxLength(ml)_
TableInfo(toMl(name), toMl(id), toMl(length), toMl(domain), toMl(data), ml)
}
tableInfoToTableStr(tableInfos)
   }

   def itemsMdToTable(url: String) = {
   def tableInfos(md: BufferedSource) = md.getLines.toList.drop(7).filter(!_.isEmpty).map{ line => val items = line.split('|').drop(1).map(_.trim)
 val (id, name, domain, length) = (items(0), items(1), items(2), items(3))
 val data = (domain.toLowerCase match {
         case "string" | "varchar2" | "char" => "x" * length.toInt
         case "timestamp" | "日付時刻" => "00010101000000"
         case "date" => "00010101"
         case "bigdecimal" | "number" | "decimal" => "0"
         case _ => DomainProcessor.exec(domain, "x" * length.toInt).right.get
      }).mkString("\"", "", "\"")
 val ml = Seq(toLength(name), toLength(id), toLength(length), toLength(domain), toLength(data)).max
 val toMl = toMaxLength(ml)_
TableInfo(toMl(name), toMl(id), toMl(length), toMl(domain), toMl(data), ml)
}
Try{Source.fromURL(s"${ url }?private_token=${ sys.env("GITLAB_TOKEN") }")}.map( md =>tableInfoToTableStr(tableInfos(md))).toOption.orElse{
      System.err.println(s"Specific is not found[$url]")
None
      }
   }

   def makeSchema(colNames: Seq[String], domains: Seq[String], colLengths: Seq[String]) = StructType(colNames.zip(colLengths).zip(domains.map(_.toLowerCase)).map{
   case ((name, _), "string") => StructField(name, StringType, true)
   case ((name, _), "varchar2") => StructField(name, StringType, true)
   case ((name, _), "char") => StructField(name, StringType, true)
   case ((name, _), "date") => StructField(name, DateType, true)
   case ((name, _), "timestamp") => StructField(name, TimestampType, true)
   case ((name, _), "日付時刻") => StructField(name, TimestampType, true)
   case ((name, _), "int") => StructField(name, IntegerType, true)
   case ((name, _), "integer") => StructField(name, IntegerType, true)
   case ((name, len), "bigdecimal") => makeBdField(len, name)
   case ((name, len), "numeric") => makeBdField(len, name)
   case ((name, len), "decimal") => makeBdField(len, name)
   case ((name, len), "number") => makeBdField(len, name)
   case ((name, _), _) => StructField(name, StringType, true)
   })

   protected def toLength(str: String) = str.getBytes("MS932").length

   protected def toMaxLength(maxLen: Int)(str: String) = {
   val addLen = maxLen - str.getBytes("MS932").length
str + (" " * addLen)
   }

   protected def tableColToList(cols: String, defaultValue: String = "") = cols.split('|').map{ col => val value = col.trim
if (value.isEmpty)
      defaultValue
else
      value
}.toList

   protected def removeDq(target: String) = "^\"(.*)\"$".r.findFirstMatchIn(target).map(_.group(1).replace("\"\"", "\"")).getOrElse(target)

   protected def toTableCol(strSeq: Seq[String]) = strSeq.mkString("| ", " | ", " |")

   protected def tableInfoToTableStr = (tableInfos: Seq[d2k.common.TableInfo]) =>{
   val names = tableInfos.map(_.itemName)
 val ids = tableInfos.map(_.itemId)
 val lengths = tableInfos.map(_.length)
 val domains = tableInfos.map(_.cnvType)
 val dataList = tableInfos.map(_.data)
 val tableSep = tableInfos.map("-" * _.maxLen)
Seq(ids, tableSep, names, lengths, domains, dataList).map(toTableCol).mkString("\n")
   }

   protected def parseItemConf(confPath: String) = {
   ConfParser.readConf(confPath){ items =>if (items.size == 5)
         {
         ItemConf(items(0), items(1), items(2), items(3), items(4).toLowerCase == "true")
         }
else
         {
         ItemConf(items(0), items(1), items(2), items(3), items(4).toLowerCase == "true", items(5))
         }
}.toSeq
   }
}
case class MakeResource (outputPath: String, readMdPath: String = "test/markdown") {
   import MakeResource._

   type SortKeys = Option[DataFrame => Seq[Column]]

   def readMdTable(path: String) : MdInfo = {
   println(s"read:${ readMdPath }/${ path }")
MdInfo(Source.fromFile(s"${ readMdPath }/${ path }").getLines.mkString("\n"))
   }

   case class MdInfo (data: String) {
      def toCsv(writeName: String, wrapDoubleQuote: Boolean = false, hasHeader: Boolean = false, lineSeparator: String = "\n") = toVariable(",")(writeName, wrapDoubleQuote, hasHeader, lineSeparator)

      def toTsv(writeName: String, wrapDoubleQuote: Boolean = false, hasHeader: Boolean = false, lineSeparator: String = "\n") = toVariable("\t")(writeName, wrapDoubleQuote, hasHeader, lineSeparator)

      def toVsv(writeName: String, wrapDoubleQuote: Boolean = false, hasHeader: Boolean = false, lineSeparator: String = "\n") = toVariable("|")(writeName, wrapDoubleQuote, hasHeader, lineSeparator)

      def toSsv(writeName: String, wrapDoubleQuote: Boolean = false, hasHeader: Boolean = false, lineSeparator: String = "\n") = toVariable(" ")(writeName, wrapDoubleQuote, hasHeader, lineSeparator)

      private [this] def toVariable(separator: String)(writeName: String, wrapDoubleQuote: Boolean, hasHeader: Boolean, lineSeparator: String) = {
      val allData = data.stripMargin.split("\n").drop(1)
 val itemNames = tableColToList(allData(0))
 val writeData = allData.drop(5).map(tableColToList(_).map( x 
 =>if (wrapDoubleQuote)
            x
else
            removeDq(x)).mkString(separator))
System.setProperty("line.separator", lineSeparator)
FileCtl.writeToFile(s"$outputPath/$writeName", false){ pw =>if (hasHeader)
            pw.println(itemNames.mkString(separator))
writeData.foreach(pw.println)
}
      }

      def toFixed(writeName: String, hasHeader: Boolean = false, hasFooter: Boolean = false, lineBreak: Boolean = true, lineSeparator: String = "\n", charEnc: String = "MS932") = writeBinData(writeName, lineSeparator){(data: String, len: String, dataType: String) =>dataType match {
         case xif x.endsWith("_PD") => pack(BigDecimal(data).toBigInt, BigDecimal(len).toInt)
         case xif x.endsWith("_ZD") => zone(BigDecimal(data).toBigInt, BigDecimal(len).toInt)
         case "数字" => s"%0${ len }d".format(data.toInt).getBytes(charEnc)
         case "数字_SIGNED" => s"%+0${ len }d".format(data.toInt).getBytes(charEnc)
         case "未使用" => (" " * BigDecimal(len).toInt).getBytes(charEnc)
         case _ => data.padTo(len.toInt, ' ').getBytes(charEnc)
      }
}

      def toJef(writeName: String, lineSeparator: String = "\n") = writeBinData(writeName, lineSeparator){(data: String, len: String, dataType: String) =>dataType match {
         case xif x.endsWith("_PD") => pack(BigDecimal(data).toBigInt, BigDecimal(len).toInt)
         case xif x.endsWith("_ZD") => zone(BigDecimal(data).toBigInt, BigDecimal(len).toInt)
         case "全角文字列" => JefConvert(data).toJefFull
         case "未使用" => JefConvert(" " * BigDecimal(len).toInt).toJefHalf
         case _ => JefConvert(data).toJefHalf
      }
}

      private [this] def writeBinData(writeName: String, lineSeparator: String)(func: (String, String, String) => Array[Byte]) = {
      val allData = data.stripMargin.split("\n")
 val itemNames = tableColToList(allData(1))
 val itemTypes = tableColToList(allData(5))
 val itemLengths = tableColToList(allData(4))
 val outData = allData.drop(6).map{
tableColToList(_).zip(itemLengths).zip(itemTypes).map{
         case ((data, len), types) => func(removeDq(data), len.split(',').head.trim, types)
         }.foldLeft(Array[Byte]())(_ ++: _)
}
writeBytes(s"$outputPath/$writeName")(outData)
      }

      private [this] def zone(target: BigInt, zonedByteLen: Int) = {
      val sign = if (target < 0)
            {
            'd'
            }
else
            {
            'f'
            }
 val targetStr = String.valueOf(target.abs)
 val pad = "0" * (zonedByteLen - targetStr.length())
 val targetBytes = (pad + targetStr).init.map{ char =>Integer.parseInt(s"f${ char }", 16).toByte}
(targetBytes :+ Integer.parseInt(s"f${ sign }${ targetStr.last }", 16).toByte).toArray
      }

      private [this] def pack(target: BigInt, packedByteLen: Int) = {
      val sign = if (target < 0)
            {
            'd'
            }
else
            {
            'f'
            }
 val targetStr = (String.valueOf(target.abs)) + sign
 val pad = "0" * ((packedByteLen * 2) - targetStr.length())
 val targetBytes = (pad + targetStr).grouped(2).map{ hexChar =>Integer.parseInt(hexChar, 16).toByte}
targetBytes.toArray
      }

      private [this] def writeBytes(path: String, lineBreak: Boolean = false, newLineCode: String = "\n")(contents: Array[Array[Byte]]) = {
      val out = new FileOutputStream (path)
contents.foreach{ arr =>out.write(arr)
if (lineBreak)
            out.write(newLineCode.toCharArray.map(_.toByte))
}
out.close
      }

      def toDf = {
      val dataTable = data.stripMargin.split("\n").drop(1)
 val colNames = tableColToList(dataTable(0))
 val colLengths = tableColToList(dataTable(3), "10")
 val domains = tableColToList(dataTable(4), "String").map(_.toLowerCase)
 val strDataList = dataTable.drop(5).map(tableColToList(_).map(removeDq))
 val schema = MakeResource.makeSchema(colNames, domains, colLengths)
 val rows = strDataList.map{ strData => val items = strData.zip(domains).map{ x => val (data, domain) = x
if (data.toLowerCase == "null")
            null
else
            {
            import MakeDate.implicits._
domain.toLowerCase match {
                  case "string" | "varchar2" | "char" | "文字列" => data
                  case "date" => data.toDt
                  case "timestamp" | "日付時刻" => data.toTm
                  case "int" | "bigdecimal" | "numeric" | "decimal" | "number" => new java.math.BigDecimal (data)
               }
            }
}
Row(items :_*)
}
 val df = SparkContexts.context.createDataFrame(rows.toList.asJava, schema)
df.show
df
      }

      def toPq(name: String) : Unit = toPq(new PqCtl (outputPath), name)

      def toPq(pqCtl: PqCtl, name: String) = {
      import pqCtl.implicits._
toDf.writeParquet(name)
      }

      def toDb(tableName: String, dbInfo: DbInfo = DbConnectionInfo.bat1) : Unit = toDb(new DbCtl (dbInfo), tableName)

      def toDb(dbCtl: DbCtl, tableName: String) = {
      import dbCtl.implicits._
Try{dbCtl.dropTable(tableName)}
toDf.writeTableStandard(tableName, SaveMode.Append)
      }

      def checkCsv(filePath: String, hasHeader: Boolean = false) = checkVariable(",")(filePath, hasHeader)

      def checkTsv(filePath: String, hasHeader: Boolean = false) = checkVariable("\t")(filePath, hasHeader)

      private [this] def checkVariable(separator: String)(filePath: String, hasHeader: Boolean = false) = {
      val fileList = Source.fromFile(filePath).getLines.toList
 val target = (if (hasHeader)
            fileList.drop(1)
else
            fileList)
 val expect = toDf.collect
target.zip(expect.zipWithIndex).map{
         case (t, (e, idx)) => val splitted = t.split(separator).map(removeDq)
 val fieldNames = e.schema.fieldNames
(0 until splitted.length).foreach{ pos =>withClue((s"LineNo:${ idx + 7 }", fieldNames(pos))){
splitted(pos).toString mustBe e(pos).toString
}
}
         }
      }

      def checkFixed(filePath: String, hasHeader: Boolean = false, hasFooter: Boolean = false) = {
      val fileList = Source.fromFile(filePath).getLines.toList
 val headerChecked = if (hasHeader)
            fileList.drop(1)
else
            fileList
 val target = (hasHeader, hasFooter) match {
            case (true, true) => fileList.drop(1).dropRight(1)
            case (true, false) => fileList.drop(1)
            case (false, true) => fileList.dropRight(1)
            case (false, false) => fileList
         }
 val dataTable = data.stripMargin.split("\n").drop(1)
 val colLengths = tableColToList(dataTable(3)).map(_.toInt)
 val expect = toDf.collect
target.zip(expect.zipWithIndex).map{
         case (t, (e, idx)) => val splitted = colLengths.foldLeft((t.getBytes("MS932"), Seq.empty[String])){(l, r) =>(l._1.drop(r), l._2 :+ new String (l._1.take(r), "MS932"))
}._2
 val fieldNames = e.schema.fieldNames
(0 until splitted.length).foreach{ pos =>withClue((s"LineNo:${ idx + 7 }", fieldNames(pos))){
splitted(pos).toString mustBe e(pos).toString
}
}
         }
      }

      def sortDf(target: DataFrame, expect: DataFrame, sortKeys: Seq[String]) = {
      target.show
 val result = if (!sortKeys.isEmpty)
            {
            (target.sort(sortKeys.head, sortKeys.tail :_*), expect.sort(sortKeys.head, sortKeys.tail :_*))
            }
else
            {
            (target, expect)
            }
 val expectCollect = result._2.collect
result._1.show(expectCollect.size, false)
testingRows(result._1.collect, expect.rdd.zipWithIndex.sortBy( x =>sortKeys.map( k =>x._1.getAs[String](k)).mkString).collect)
      }

      def checkPq(name: String) : Unit = checkPq(new PqCtl (outputPath), name, Seq.empty[String])

      def checkPq(name: String, sortKeys: Seq[String]) : Unit = checkPq(new PqCtl (outputPath), name, sortKeys)

      def checkPq(pqCtl: PqCtl, name: String, sortKeys: Seq[String] = Seq.empty[String]) = sortDf(pqCtl.readParquet(name), toDf, sortKeys)

      def checkDb(tableName: String, sortKeys: Seq[String] = Seq.empty[String], dbInfo: DbInfo = DbConnectionInfo.bat1) : Unit = checkDb(new DbCtl (dbInfo), tableName, sortKeys)

      def checkDb(dbCtl: DbCtl, tableName: String, sortKeys: Seq[String]) = sortDf(dbCtl.readTable(tableName), toDf, sortKeys)

      def checkDf(df: DataFrame, lineOffset: Int = 0) = testingRows(df.collect, toDf.rdd.zipWithIndex.collect, lineOffset)

      private [this] def testingRows(target: Array[Row], expect: Array[(Row, Long)], lineOffset: Int = 0) = {
      val fieldNames = expect.headOption.map(_._1.schema.fieldNames).getOrElse(Array.empty[String])
 val expectTypes = expect.headOption.map(_._1.schema.fields.map(_.dataType.toString)).getOrElse(Array.empty[String])
 val targetTypes = target.headOption.map(_.schema.fields.map( x =>(x.name, x.dataType.toString)).toMap).getOrElse(Map.empty[String, String])
 val systemItems = Seq("DT_D2KMKDTTM", "ID_D2KMKUSR", "DT_D2KUPDDTTM", "ID_D2KUPDUSR", "NM_D2KUPDTMS", "FG_D2KDELFLG")
if (target.isEmpty)
            logger.warn("Target Data is Empty")
target.zip(expect).map{
         case (t, (e, idx)) => (0 until e.length).foreach{ pos => val name = fieldNames(pos)
if (!systemItems.contains(name))
            {
            withClue((s"LineNo:${ idx + 7 + lineOffset }", name)){
expectTypes(pos) match {
                  case "DateType" => t.getAs[Any](name).toString.replaceAll("-", "").take(8) mustBe e.getAs[Any](name).toString.replaceAll("-", "").take(8)
                  case "TimestampType" => t.getAs[Timestamp](name).toString.replaceAll("[-:\\s\\.]", "").take(14) mustBe e.getAs[Timestamp](name).toString.replaceAll("[-:\\s\\.]", "").take(14)
                  case "IntegerType" => val targetData = if (targetTypes(name).startsWith("Integer"))
                     {
                     t.getAs[Integer](name)
                     }
else
                     {
                     t.getAs[java.math.BigDecimal](name)
                     }
Option(targetData).map(_.toString).getOrElse("null") mustBe Option(e.getAs[Integer](name)).map(_.toString).getOrElse("null")
                  case typif typ.startsWith("DecimalType") => {
                  val targetData = if (targetTypes(name).startsWith("Integer"))
                        {
                        t.getAs[Integer](name)
                        }
else
                        {
                        t.getAs[java.math.BigDecimal](name)
                        }
Option(targetData).map(_.toString).getOrElse("null") mustBe Option(e.getAs[java.math.BigDecimal](name)).map(_.toString).getOrElse("null")
                  }
                  case _ => {
                  val targetVal = t.getAs[String](name)
(if (targetVal == null)
                        ""
else
                        targetVal) mustBe e.getAs[String](name)
                  }
               }
}
            }
}
         }
      }
   }
}