package d2k.app.test.common

import java.sql.Timestamp
import java.time.LocalDateTime
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.Row
import spark.common.SparkContexts
import d2k.common.fileConv.Converter
import d2k.common.fileConv.ConfParser
import d2k.common.fileConv.ItemConf
import spark.common.FileCtl
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object MakeDf {
   trait MakeDf {
      def allSpace: DataFrame

      def allEmpty: DataFrame
   }

   def apply(confPath: String) = new Plane (confPath)

   def dc(confPath: String) = new DomainConverter (confPath)

   def makeInputDf(rows: Seq[Row], names: Seq[String], domains: Seq[String]) = {
   val rdd = SparkContexts.sc.makeRDD(rows)
 val ziped = names.zip(domains)
 val (nameList, domainList) = ziped.filter{
      case (names, domain) => !(domain.startsWith(Converter.NOT_USE_PREFIX) || domain.startsWith(Converter.REC_DIV_PREFIX))
      }.unzip
SparkContexts.context.createDataFrame(rdd, Converter.makeSchema(nameList))
   }

   def parseItemConf(confPath: String) = {
   val conf = ConfParser.readConf(confPath){ items =>if (items.size == 5)
         {
         ItemConf(items(0), items(1), items(2), items(3), items(4).toLowerCase == "true")
         }
else
         {
         ItemConf(items(0), items(1), items(2), items(3), items(4).toLowerCase == "true", items(5))
         }
}.toSeq
(conf, conf.map(_.itemId), conf.map(_.cnvType))
   }

   class Plane (confPath: String) extends MakeDf {
      val (itemConfs, names, domains) = parseItemConf(confPath)

      def allSpace = exec{(ic: ItemConf, cnt: Int) =>" " * ic.length.toInt}

      def allEmpty = exec{(ic: ItemConf, cnt: Int) =>""}

      private [this] def exec(func: (ItemConf, Int) => String) = {
      val orgData = itemConfs.zipWithIndex.map{
         case (ic, cnt) => func(ic, cnt)
         }
makeInputDf(Seq(Row(orgData :_*)), names, domains)
      }
   }

   class DomainConverter (confPath: String) extends MakeDf {
      val (itemConfs, names, domains) = parseItemConf(confPath)

      def allSpace = exec{(ic: ItemConf, cnt: Int) =>" " * ic.length.toInt}

      def allEmpty = exec{(ic: ItemConf, cnt: Int) =>""}

      private [this] def exec(func: (ItemConf, Int) => String) = {
      val orgData = itemConfs.zipWithIndex.map{
         case (ic, cnt) => func(ic, cnt)
         }
 val zipped = orgData.zip(domains).zip(names)
 val convedData = Converter.domainConvert(zipped)
makeInputDf(Seq(Row(convedData :_*)), names, domains)
      }
   }

   implicit class DataFrameConverter (df: DataFrame) {
      def writeFixedFile(writePath: String, append: Boolean = false, header: Boolean = false, footer: Boolean = false, newLine: Boolean = true, lineSeparator: String = "\n") = {
      System.setProperty("line.separator", lineSeparator)
FileCtl.writeToFile(writePath, append){ pw =>df.collect.map(_.mkString).foreach{ x =>if (header)
            pw.println(" " * x.mkString.length)
if (newLine)
            pw.println(x.mkString)
else
            pw.print(x.mkString)
if (footer)
            pw.println(" " * x.mkString.length)
}
}
      }
   }
}