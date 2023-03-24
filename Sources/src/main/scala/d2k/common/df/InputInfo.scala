package d2k.common.df

import scala.util.Try
import FileInputInfoBase._
import org.apache.spark.sql.Column

sealed trait InputInfo
object FileInputInfoBase {
  val ENV_NAME_DEFAULT = "DEFAULT"

  sealed trait NewLineCode
  case object CR extends NewLineCode
  case object LF extends NewLineCode
  case object CRLF extends NewLineCode

}

case class CsvInfo(
  inputFiles: Set[String], envName: String = FileInputInfoBase.ENV_NAME_DEFAULT, header: Boolean = false, charSet: String = "MS932",
  itemConfId: String = "", dropRowError: Boolean = true) extends VariableInputInfoBase {
  val fileFormat = "csv"
}

case class TsvInfo(
  inputFiles: Set[String], envName: String = FileInputInfoBase.ENV_NAME_DEFAULT, dropDoubleQuoteMode: Boolean = false, header: Boolean = false, charSet: String = "MS932",
  itemConfId: String = "", dropRowError: Boolean = true) extends VariableInputInfoBase {
  val fileFormat = if (dropDoubleQuoteMode) "tsvDropDoubleQuote" else "tsv"
}

case class VsvInfo(
  inputFiles: Set[String], envName: String = FileInputInfoBase.ENV_NAME_DEFAULT, header: Boolean = false, charSet: String = "MS932",
  itemConfId: String = "", dropRowError: Boolean = true) extends VariableInputInfoBase {
  val fileFormat = "vsv"
}

case class SsvInfo(
  inputFiles: Set[String], envName: String = FileInputInfoBase.ENV_NAME_DEFAULT, header: Boolean = false, charSet: String = "MS932",
  itemConfId: String = "", dropRowError: Boolean = true) extends VariableInputInfoBase {
  val fileFormat = "ssv"
}

case class FixedInfo(inputFiles: Set[String], envName: String = FileInputInfoBase.ENV_NAME_DEFAULT,
                     header: Boolean = false, footer: Boolean = false, newLine: Boolean = true,
                     withIndex: Boolean = false, recordLengthCheck: Boolean = false, charSet: String = "MS932", newLineCode: NewLineCode = LF,
                     itemConfId: String = "", dropRowError: Boolean = true, preFilter: (Seq[String], Map[String, String] => Boolean) = null,
                     withBinaryRecord: String = "") extends FileInputInfoBase {
  val fileFormat = "fixed"
}

trait PqInputInfoBase extends InputInfo {
  val pqName: String
  val envName: String
  def inputDir(componentId: String): String = Try { sys.env(s"PQ_INPUT_PATH_${componentId}") }.
    getOrElse(Try { sys.env(s"PQ_INPUT_PATH_${envName}") }.getOrElse(sys.env(s"PQ_INPUT_PATH_${ENV_NAME_DEFAULT}")))
}
case class PqInfo(pqName: String, envName: String = "") extends PqInputInfoBase
case class VariableJoin(inputInfo: InputInfo, joinExprs: Column, prefixName: String = "", dropCols: Set[String] = Set.empty[String])

trait FileInputInfoBase extends InputInfo {

  val fileFormat: String

  val envName: String
  def inputDir(componentId: String): String = Try { sys.env(s"FILE_INPUT_PATH_${componentId}") }.
    getOrElse(Try { sys.env(s"FILE_INPUT_PATH_${envName}") }.getOrElse(sys.env(s"FILE_INPUT_PATH_${ENV_NAME_DEFAULT}")))
  val inputFiles: Set[String]

  val newLine: Boolean
  val withIndex: Boolean
  val recordLengthCheck: Boolean
  val header: Boolean
  val charSet: String
  /** for DfJoinVariableToDf */
  val itemConfId: String
  /** for DfJoinVariableToDf */
  val dropRowError: Boolean
}

trait VariableInputInfoBase extends FileInputInfoBase {
  override val newLine = true
  override val withIndex = false
  override val recordLengthCheck = false
}

