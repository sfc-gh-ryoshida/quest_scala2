package d2k.common.df

import spark.common.DbInfo
import spark.common.PqCtl
import d2k.common.InputArgs
import d2k.common.ResourceInfo

object ReadPq {
  def toSchema(names: Seq[String]) = names.map { name =>
    name.split("_").toList.headOption.map {
      case "DT"        => "date"
      case "NM" | "AM" => "decimal"
      case _           => "string"
    }
  }
}

trait ReadPq extends ResourceInfo {
  def readPqPath(implicit inArgs: InputArgs): String = inArgs.baseInputFilePath
  /**
   * ファイルが存在しなかった場合の挙動を決定する
   * true: Exceptionが発生(default)
   * false: ログにWarningが出力され空のDataFrameが返る
   */
  val readPqStrictCheckMode: Boolean = true

  /**
   * readPqStrictCheckModeでfalseが指定された場合で、ParquetがNotFoundだった場合のschemaを指定する
   */
  val readPqEmptySchema: Seq[(String, String)] = Seq.empty[(String, String)]

  def readParquetSingle(pqName: String)(implicit inArgs: InputArgs) = {
    val pqCtl = new PqCtl(readPqPath)
    pqCtl.readParquet(pqName, readPqStrictCheckMode, readPqEmptySchema)
  }
}