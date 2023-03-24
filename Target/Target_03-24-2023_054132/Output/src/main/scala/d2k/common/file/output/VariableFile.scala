/*
EWI: SPRKSCL1001 => This code section has parsing errors, so it was commented out
package d2k.common.file.output

import org.apache.spark.sql.DataFrame
import d2k.common.InputArgs
import spark.common.FileCtl
import java.nio.file.Files
import java.nio.file.Path
import java.io.File
import java.io.FileSystem
import java.nio.file.FileSystems
import org.apache.spark.sql.Row
import scala.reflect.io.Directory
import spark.common.DfCtl
import spark.common.SparkContexts
import DfCtl.implicits._
import SparkContexts.context.implicits._
import d2k.common.Logging
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SaveMode

object VariableFile {
  def addDoubleQuote(target: String, wrapDoubleQuote: Boolean, escapeChar: String) = {
    if (wrapDoubleQuote) {
      val escaped = target.toString.replaceAll("\"", s"""${escapeChar}"""")
      s""""${escaped}""""
    } else { target }
  }

  def mkOutputStr(row: Row, separator: String, wrapDoubleQuote: Boolean, inEscapeChar: String) = {
    val escapeChar = if (inEscapeChar.isEmpty) "\"" else inEscapeChar
    row.toSeq.map { col =>
      addDoubleQuote(Option(col).map(_.toString).getOrElse(""), wrapDoubleQuote, escapeChar)
    }.mkString(separator)
  }

  def mkOutputBinary(row: Row, separator: String, wrapDoubleQuote: Boolean, escapeChar: String, charEnc: String) =
    mkOutputStr(row, separator, wrapDoubleQuote, escapeChar).getBytes(charEnc)

  def mkOutputCsvStr(row: Row, targetColumns: Set[String], wrapDoubleQuote: Boolean, escapeChar: String) = {
    row.toSeq.zip(row.schema.fieldNames).map {
      case (col, name) =>
        if (targetColumns.exists(_ == name)) {
          Option(col).map(str => addDoubleQuote(str.toString, true, escapeChar)).getOrElse(addDoubleQuote("", wrapDoubleQuote, escapeChar))
        } else {
          Option(col).map(_.toString).getOrElse("")
        }
    }.mkString(",")
  }

  def mkOutputCsvBinary(row: Row, targetColumns: Set[String], wrapDoubleQuote: Boolean, escapeChar: String, charEnc: String) =
    mkOutputCsvStr(row, targetColumns, wrapDoubleQuote, escapeChar).getBytes(charEnc)
}

class VariableFile(
    fileName: String, wrapDoubleQuote: Boolean, escapeChar: String, charEnc: String,
    writeFilePartitionColumns: Seq[String] = Seq.empty[String], writeFilePartitionExtention: String = "") extends Logging {
  import VariableFile._

  def write(df: DataFrame, fileName: String, separator: String = ",") = {
    df.map(_.mkString(separator)).rdd.saveAsTextFile(fileName)
  }

  def writeSingle(separator: String) = (df: DataFrame) => {
    if (writeFilePartitionColumns.isEmpty) {
      Directory(fileName).createDirectory(true, false)
      Files.deleteIfExists(FileSystems.getDefault.getPath(fileName))
      FileCtl.writeToFile(fileName, charEnc = charEnc) { pw =>
        val collected = df.collect
        elapse(s"fileWrite:${fileName}") {
          collected.foreach { row => pw.println(mkOutputStr(row, separator, wrapDoubleQuote, escapeChar)) }
        }
      }
    } else {
      FileCtl.deleteDirectory(fileName)
      elapse(s"fileWrite:${fileName}") {
        FileCtl.loanPrintWriterCache { cache =>
          df.collect.foldLeft(cache) { (l, r) =>
            FileCtl.writeToFileWithPartitionColumns(
              fileName, partitionColumns = writeFilePartitionColumns, partitionExtention = writeFilePartitionExtention)(
                row => mkOutputStr(row, separator, wrapDoubleQuote, escapeChar))(l)(r)
          }
        }
      }
    }
  }

  def writeHdfs(separator: String) = (df: DataFrame) => {
    val paraCheckedDf = if (writeFilePartitionColumns.isEmpty) {
      df.write
    } else {
      df.write.partitionBy(writeFilePartitionColumns: _*)
    }

    val ops = Seq(("delimiter", separator), ("encoding", charEnc),
      ("ignoreLeadingWhiteSpace", "false"), ("ignoreTrailingWhiteSpace", "false"))
    val ops2 = if (wrapDoubleQuote) ops :+ ("quoteAll", "true") else ops :+ ("emptyValue", "")
    val ops3 = if (!escapeChar.isEmpty) ops2 :+ ("escape", escapeChar) else ops2
    paraCheckedDf.mode(SaveMode.Overwrite).options(ops3.toMap).csv(fileName)
  }

  def writePartition(separator: String) = (df: DataFrame) => {
    if (writeFilePartitionColumns.isEmpty) {
      df.partitionWriteFile(fileName, true, charEnc, writeFilePartitionExtention)(row => mkOutputStr(row, separator, wrapDoubleQuote, escapeChar))
    } else {
      FileCtl.deleteDirectory(fileName)
      df.partitionWriteToFileWithPartitionColumns(fileName, writeFilePartitionColumns, true, charEnc, writeFilePartitionExtention)(row => mkOutputStr(row, separator, wrapDoubleQuote, escapeChar))
    }
  }

  def writeSequence(separator: String) = (df: DataFrame) => {
    FileCtl.deleteDirectory(fileName)
    df.rdd.map(row => (NullWritable.get, mkOutputBinary(row, separator, wrapDoubleQuote, escapeChar, charEnc)))
      .saveAsSequenceFile(fileName, Some(classOf[org.apache.hadoop.io.compress.SnappyCodec]))
  }

  def writeSingleCsvWithDoubleQuote(targetColumns: Set[String]) = (df: DataFrame) => {
    if (writeFilePartitionColumns.isEmpty) {
      Directory(fileName).createDirectory(true, false)
      Files.deleteIfExists(FileSystems.getDefault.getPath(fileName))
      FileCtl.writeToFile(fileName, charEnc = charEnc) { pw =>
        val collected = df.collect
        elapse(s"fileWrite:${fileName}") {
          collected.foreach(row => pw.println(mkOutputCsvStr(row, targetColumns, wrapDoubleQuote, escapeChar)))
        }
      }
    } else {
      FileCtl.deleteDirectory(fileName)
      elapse(s"fileWrite:${fileName}") {
        FileCtl.loanPrintWriterCache { cache =>
          df.collect.foldLeft(cache) { (l, r) =>
            FileCtl.writeToFileWithPartitionColumns(
              fileName, partitionColumns = writeFilePartitionColumns, partitionExtention = writeFilePartitionExtention)(
                row => mkOutputCsvStr(row, targetColumns, wrapDoubleQuote, escapeChar))(l)(r)
          }
        }
      }
    }
  }

  def writePartitionCsvWithDoubleQuote(targetColumns: Set[String]) = (df: DataFrame) => {
    if (writeFilePartitionColumns.isEmpty) {
      df.partitionWriteFile(fileName, true, charEnc, partitionExtention = writeFilePartitionExtention)(row => mkOutputCsvStr(row, targetColumns, wrapDoubleQuote, escapeChar))
    } else {
      FileCtl.deleteDirectory(fileName)
      df.partitionWriteToFileWithPartitionColumns(fileName, writeFilePartitionColumns, true, charEnc, writeFilePartitionExtention)(row => mkOutputCsvStr(row, targetColumns, wrapDoubleQuote, escapeChar))
    }
  }

  def writeSequenceCsvWithDoubleQuote(targetColumns: Set[String]) = (df: DataFrame) => {
    FileCtl.deleteDirectory(fileName)
    df.rdd.map(row => (NullWritable.get, mkOutputCsvBinary(row, targetColumns, wrapDoubleQuote, escapeChar, charEnc)))
      .saveAsSequenceFile(fileName, Some(classOf[org.apache.hadoop.io.compress.SnappyCodec]))
  }
}

*/