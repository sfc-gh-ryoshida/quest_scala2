package d2k.common.fileConv

import org.apache.spark.sql.Row

import java.io.BufferedInputStream

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import spark.common.SparkContexts._
import d2k.common.df.FileInputInfoBase._
import org.apache.spark.sql.types._

class FixedConverter(hasHeader: Boolean = false, hasFooter: Boolean = false, charEnc: String = "MS932", lineBreak: Boolean = true, newLineCode: NewLineCode = LF,
                     withIndex: Boolean = false, preFilter: (Seq[String], Map[String, String] => Boolean) = null,
                     withBinaryRecord: Option[String] = None) extends Serializable {
  import Converter._

  def makeSliceLen(len: Seq[Int]) = len.foldLeft((0, List.empty[(Int, Int)])) { (l, r) => (l._1 + r, l._2 :+ (l._1, l._1 + r)) }

  def cnvFromFixed(names: Seq[String], domains: Seq[String], sliceLen: List[(Int, Int)])(inData: Array[Byte]) = {
    val dataAndDomainsAndNames = sliceLen.map { case (start, end) => inData.slice(start, end) }.zip(domains).zip(names)
    val result = Converter.domainConvert(dataAndDomainsAndNames, charEnc)
    Row(result: _*)
  }

  def cnvFromFixedWithIndex(names: Seq[String], domains: Seq[String], sliceLen: List[(Int, Int)])(inData: (Array[Byte], Long)) = {
    val dataAndDomainsAndNames = sliceLen.map {
      case (start, end) if start == -1 && end == -1 => inData._2.toString.getBytes
      case (start, end)                             => inData._1.slice(start, end)
    }.zip(domains).zip(names)
    val result = Converter.domainConvert(dataAndDomainsAndNames, charEnc)
    Row(result: _*)
  }

  def cnvFromFixedAddAllData(names: Seq[String], domains: Seq[String], sliceLen: List[(Int, Int)])(inData: Array[Byte]) = {
    val dataAndDomainsAndNames = sliceLen.map { case (start, end) => inData.slice(start, end) }.zip(domains).zip(names)
    val result = Seq(new String(inData, charEnc)) ++ Converter.domainConvert(dataAndDomainsAndNames, charEnc)
    Row(result: _*)
  }

  def addLineBreak(totalLen: Int, lineBreak: Boolean) = (lineBreak, newLineCode) match {
    case (false, _)   => totalLen
    case (true, CR)   => totalLen + 1
    case (true, LF)   => totalLen + 1
    case (true, CRLF) => totalLen + 2
  }

  def addIndexColumn(names: Seq[String], nameList: Seq[String], domains: Seq[String], sliceLen: List[(Int, Int)]) =
    (names :+ SYSTEM_COLUMN_NAME.RECORD_INDEX,
      nameList :+ SYSTEM_COLUMN_NAME.RECORD_INDEX,
      domains :+ "文字列",
      sliceLen :+ (-1, -1))

  def arrToMap(targetNames: Seq[String], names: Seq[String], domains: Seq[String], sliceLen: List[(Int, Int)])(inData: Array[Byte]): Map[String, String] = {
    val dataAndDomainsAndNames = sliceLen.map { case (start, end) => inData.slice(start, end) }.zip(domains).zip(names)
    val targetData = targetNames.flatMap { target =>
      dataAndDomainsAndNames.filter(_._2 == target)
    }
    val converted = Converter.domainConvert(targetData, charEnc)
    targetData.map(_._2).zip(converted).toMap
  }

  def addArr(row: Row, arr: Array[Byte]) = {
    val rowValue = Row(row.toSeq: _*)
    withBinaryRecord.map(_ => Row.merge(rowValue, Row(arr.clone))).getOrElse(rowValue)
  }

  def addStructType(st: StructType) =
    withBinaryRecord.map(n => st.add(StructField(n, BinaryType))).getOrElse(st)

  def makeInputDf(len: Seq[Int], names: Seq[String], domains: Seq[String], filePath: String) = {
    val (totalLen_, sliceLen) = makeSliceLen(len)
    val totalLen = addLineBreak(totalLen_, lineBreak)
    val ziped = names.zip(domains)
    val (nameList, domainList) = ziped.filter { case (names, domain) => !(domain.startsWith(Converter.NOT_USE_PREFIX)) }.unzip

    val rdd = Option(preFilter).map { pf =>
      sc.binaryRecords(filePath, totalLen).flatMap { arr =>
        if (pf._2(arrToMap(pf._1, names, domains, sliceLen)(arr))) Some(arr) else None
      }
    }.getOrElse(sc.binaryRecords(filePath, totalLen))
    val df = if (withIndex) {
      if (charEnc == "JEF") { throw new IllegalArgumentException("JEF CharEnc is not supportted") }
      val rddWithIdx = rdd.zipWithIndex
      val (namesWithIdx, nameListWithIdx, domainsWithIdx, sliceLenWithIdx) = addIndexColumn(names, nameList, domains, sliceLen)
      context.createDataFrame(rddWithIdx.map {
        case (arr, long) => addArr(cnvFromFixedWithIndex(namesWithIdx, domainsWithIdx, sliceLenWithIdx)(arr, long), arr)
      }, addStructType(Converter.makeSchema(nameListWithIdx)))
    } else {
      context.createDataFrame(rdd.map(arr =>
        addArr(cnvFromFixed(names, domains, sliceLen)(arr), arr)), addStructType(Converter.makeSchema(nameList)))
    }
    Converter.removeHeaderAndFooter(df, hasHeader, hasFooter, names, domains)
  }

  import scala.collection.JavaConversions._

  case class Result(buf: Array[Byte] = Array.empty[Byte], output: Array[Array[Byte]] = Array.empty[Array[Byte]], len: Int = 0)
  def recordErrorCheckAndMakeInputDf(len: Seq[Int], names: Seq[String], domains: Seq[String], filePath: String) = {
    if (charEnc == "JEF") { throw new IllegalArgumentException("JEF CharEnc is not supportted") }
    if (!lineBreak) { throw new IllegalArgumentException("must be lineBreak = true when recordLengthCheck = true") }
    val (totalLen_, sliceLen) = makeSliceLen(len)
    val totalLen = addLineBreak(totalLen_, lineBreak)
    val ziped = names.zip(domains)
    val (nameList, domainList) = ziped.filter { case (names, domain) => !(domain.startsWith(Converter.NOT_USE_PREFIX)) }.unzip

    val splitByLineBreak = {
      sc.binaryFiles(filePath).flatMap {
        case (_, pds) => {
          var buff = new Array[Byte](1)
          var resultArr = ArrayBuffer[Array[Byte]]()
          var tmpArr = ArrayBuffer[Byte]()
          val br = new BufferedInputStream(pds.open())
          while (br.read(buff) != -1) {
            val byte = buff(0)
            if (byte == '\n' || byte == '\r') {
              resultArr += tmpArr.toArray
              tmpArr = ArrayBuffer[Byte]()
            } else {
              tmpArr += byte
            }
          }
          Option(preFilter).map { pf =>
            resultArr.flatMap { arr =>
              if (pf._2(arrToMap(pf._1, names, domains, sliceLen)(arr))) Some(arr) else None
            }
          }.getOrElse(resultArr)
        }
      }
    }

    val df = if (withIndex) {
      val (namesWithIdx, nameListWithIdx, domainsWithIdx, sliceLenWithIdx) = addIndexColumn(names, nameList, domains, sliceLen)
      val r = splitByLineBreak.zipWithIndex.map {
        case (arr, idx) =>
          val row = cnvFromFixedWithIndex(namesWithIdx, domainsWithIdx, sliceLenWithIdx)(arr, idx)
          addArr(Row(row.toSeq :+ (arr.size != totalLen_).toString: _*), arr)
      }
      context.createDataFrame(r, addStructType(Converter.makeSchemaWithRecordError(nameListWithIdx)))
    } else {
      val r = splitByLineBreak.map { arr =>
        val row = cnvFromFixed(names, domains, sliceLen)(arr)
        addArr(Row(row.toSeq :+ (arr.size != totalLen_).toString: _*), arr)
      }
      context.createDataFrame(r, addStructType(Converter.makeSchemaWithRecordError(nameList)))
    }
    Converter.removeHeaderAndFooter(df, hasHeader, hasFooter, names, domains)
  }
}
