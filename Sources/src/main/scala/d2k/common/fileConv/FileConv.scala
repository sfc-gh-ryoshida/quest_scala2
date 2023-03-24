package d2k.common.fileConv

import scala.reflect.io.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.reflect.io.Path.string2path
import d2k.common.InputArgs
import spark.common.PqCtl
import d2k.common.df._
import d2k.common.df.FileInputInfoBase
import scala.util.Try
import spark.common.SparkContexts.context
import org.apache.spark.sql.Row
import java.io.FileNotFoundException
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException
import org.apache.hadoop.mapred.FileInputFormat
import spark.common.FileCtl

class FileConv(componentId: String, fileInputInfo: FileInputInfoBase, itemConfIdTemplate: String, mkEmptyDfWhenFileNotExists: Boolean = false)(implicit inArgs: InputArgs) extends Serializable {
  val dirPath = fileInputInfo.inputDir(componentId)
  val filePaths = fileInputInfo.inputFiles.map(file => (Path(dirPath) / file).toString)

  val itemConfId = if (fileInputInfo.itemConfId.isEmpty) itemConfIdTemplate else fileInputInfo.itemConfId
  val itemConfs = ConfParser.parseItemConf(Path(inArgs.fileConvInputFile).toAbsolute.parent, inArgs.projectId, itemConfId).toList
  val extractTargetsForSqlFilter = itemConfs
    .filter(_.extractTarget)
    .map(item => s"substring(${item.itemId},1,8) = ${inArgs.runningDateYMD}")
    .mkString(" or ")
  val extractSqlFilter = s"ROW_ERR = 'true' or ( ${extractTargetsForSqlFilter} )"

  def makeDf = {
    val len = itemConfs.map(_.length.toInt)
    val names = itemConfs.map(_.itemId)
    val domains = itemConfs.map(_.cnvType)

    //入力ファイル変換処理 入力ファイル⇒DataFrame
    val textConverter = new TextConverter(fileInputInfo.header, fileInputInfo.charSet)
    def makeDf = fileInputInfo.fileFormat match {
      case "tsv" => textConverter.tsv(names, domains, filePaths)
      case "csv" => textConverter.csv(names, domains, filePaths)
      case "vsv" => textConverter.vsv(names, domains, filePaths)
      case "ssv" => textConverter.ssv(names, domains, filePaths)
      case "fixed" => {
        val fi: FixedInfo = fileInputInfo.asInstanceOf[FixedInfo]
        val files = filePaths.mkString(",")
        val fc = new FixedConverter(fi.header, fi.footer, fi.charSet, fi.newLine, fi.newLineCode,
          fi.withIndex, fi.preFilter, if (fi.withBinaryRecord.isEmpty) None else Some(fi.withBinaryRecord))
        if (fi.recordLengthCheck) {
          fc.recordErrorCheckAndMakeInputDf(len, names, domains, files)
        } else {
          fc.makeInputDf(len, names, domains, files)
        }
      }
    }
    if (mkEmptyDfWhenFileNotExists && !FileCtl.exists(filePaths.mkString(","))) {
      context.createDataFrame(context.emptyDataFrame.rdd, Converter.makeSchema(names))
    } else {
      makeDf
    }
  }
}
