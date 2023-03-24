package d2k.common.fileConv

import org.apache.spark.sql.Row
import java.net.URI
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.File
import com.univocity.parsers.csv.CsvParserSettings
import com.univocity.parsers.csv.CsvParser
import com.univocity.parsers.tsv.TsvParserSettings
import com.univocity.parsers.tsv.TsvParser

import scala.collection.JavaConversions._
import spark.common.SparkContexts._
import spark.common.SparkContexts
import org.apache.spark.SparkContext

class TextConverter(hasHeader: Boolean = false, charEnc: String = "MS932") extends Serializable {
  def makeDf(options: Map[String, String])(names: Seq[String], domains: Seq[String], path: Set[String]) = {
    val rdd = SparkContexts.context.read.options(options).csv(path.toSeq: _*).rdd.map { row =>
      val dataAndDomainsAndNames = row.toSeq.map(s => Option(s).map(_.toString).getOrElse("")).zip(domains).zip(names)
      Row(Converter.domainConvert(dataAndDomainsAndNames): _*)
    }
    val ziped = names.zip(domains)
    val (nameList, domainList) = ziped.filter { case (names, domain) => !(domain.startsWith(Converter.NOT_USE_PREFIX) || domain.startsWith(Converter.REC_DIV_PREFIX)) }.unzip
    context.createDataFrame(rdd, Converter.makeSchema(nameList))
  }

  val defaultOptions = Seq(("encoding", charEnc), ("header", hasHeader.toString))
  def tsv = makeDf((defaultOptions :+ ("delimiter", "\t")).toMap) _
  val csv = makeDf(defaultOptions.toMap) _
  val vsv = makeDf((defaultOptions :+ ("delimiter", "|")).toMap) _
  val ssv = makeDf((defaultOptions :+ ("delimiter", " ")).toMap) _
}
