package d2k.common.df

import org.apache.spark.sql.DataFrame
import d2k.common.InputArgs
import d2k.common.file.output.VariableFile
import d2k.common.ResourceInfo
import d2k.common.file.output._

sealed trait WriteFileMode

object WriteFileMode {
  case object Csv extends WriteFileMode
  case class Csv(wrapTargetCols: String*) extends WriteFileMode
  case object Tsv extends WriteFileMode

  case object Fixed extends WriteFileMode
  case class Fixed(itemLengths: Int*) extends WriteFileMode

  object partition {
    case object Csv extends WriteFileMode
    case class Csv(wrapTargetCols: String*) extends WriteFileMode
    case object Tsv extends WriteFileMode
    case object Fixed extends WriteFileMode
    case class Fixed(itemLengths: Int*) extends WriteFileMode
  }

  object hdfs {
    case object Csv extends WriteFileMode
    case object Tsv extends WriteFileMode
    case object Fixed extends WriteFileMode
    case class Fixed(itemLengths: Int*) extends WriteFileMode
  }

  object sequence {
    case object Csv extends WriteFileMode
    case class Csv(wrapTargetCols: String*) extends WriteFileMode
    case object Tsv extends WriteFileMode
    case class Fixed(itemLengths: Int*) extends WriteFileMode
  }
}

trait WriteFile extends ResourceInfo {
  import WriteFileMode._

  lazy val writeFileName: String = componentId
  def writeFilePath(implicit inArgs: InputArgs): String = inArgs.baseOutputFilePath

  /**
   * File書込みモード ※暫定<br>
   * Fixed(default), Fixed(itemLengths), CsvSingle, CsvSingle(wrapTargetCols), TsvSingle
   */
  val writeFileMode: WriteFileMode = Fixed

  /**
   * 可変長出力の場合に項目をDouble Quoteで包むか
   * true:包む
   * false:包まない
   */
  val writeFileVariableWrapDoubleQuote: Boolean = true
  /**
   * 可変長出力でDouble Quoteで包む場合のDouble Quote Escape文字列
   */
  val writeFileVariableEscapeChar: String = ""

  val writeFileFunc: (DataFrame, InputArgs, Map[String, String]) => Unit = null

  val writeCharEncoding: String = "MS932"

  /**
   * パーティション出力対象項目リスト
   */
  val writeFilePartitionColumns: Seq[String] = Seq.empty[String]

  /**
   * パーティション出力ファイル拡張子
   */
  val writeFilePartitionExtention: String = ""

  def writeFile(df: DataFrame)(implicit inArgs: InputArgs) = {
    val writeFilePathAndName = s"${writeFilePath}/${writeFileName}"
    val vari = new VariableFile(writeFilePathAndName, writeFileVariableWrapDoubleQuote, writeFileVariableEscapeChar, writeCharEncoding, writeFilePartitionColumns, writeFilePartitionExtention)
    val fixed = new FixedFile(writeFilePathAndName, writeFilePartitionColumns, writeFilePartitionExtention)
    val writer = writeFileMode match {
      case Csv                                => vari.writeSingle(",")
      case Csv(wrapTargetCols @ _*)           => vari.writeSingleCsvWithDoubleQuote(wrapTargetCols.toSet)
      case Tsv                                => vari.writeSingle("\t")
      case Fixed(itemLengths @ _*)            => fixed.writeSingle_MS932(itemLengths)
      case Fixed                              => new FixedFileWithConfFile(writeFilePathAndName).writeFile(writeFileFunc)
      case partition.Csv                      => vari.writePartition(",")
      case partition.Csv(wrapTargetCols @ _*) => vari.writePartitionCsvWithDoubleQuote(wrapTargetCols.toSet)
      case partition.Tsv                      => vari.writePartition("\t")
      case partition.Fixed(itemLengths @ _*)  => fixed.writePartition_MS932(itemLengths)
      case hdfs.Csv                           => vari.writeHdfs(",")
      case hdfs.Tsv                           => vari.writeHdfs("\t")
      case hdfs.Fixed(itemLengths @ _*)       => fixed.writeHdfs_MS932(itemLengths)
      case sequence.Csv                       => vari.writeSequence(",")
      case sequence.Csv(wrapTargetCols @ _*)  => vari.writeSequenceCsvWithDoubleQuote(wrapTargetCols.toSet)
      case sequence.Tsv                       => vari.writeSequence("\t")
      case sequence.Fixed(itemLengths @ _*)   => fixed.writeSequence_MS932(itemLengths)
      case _                                  => throw new IllegalArgumentException(s"${writeFileMode} is unusable")
    }
    writer(df)
    df.sqlContext.emptyDataFrame
  }
}
