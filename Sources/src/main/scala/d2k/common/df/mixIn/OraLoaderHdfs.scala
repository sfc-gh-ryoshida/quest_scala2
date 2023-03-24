package d2k.common.df.mixIn

import d2k.common.InputArgs
import d2k.common.df.WriteFile
import d2k.common.df.WriteFileMode._

trait OraLoaderHdfs extends WriteFile {
  override val writeFileVariableWrapDoubleQuote = true
  override val writeFileVariableEscapeChar = "\""
  override val writeFileMode = hdfs.Csv
  override def writeFilePath(implicit inArgs: InputArgs) = sys.env("DB_LOADING_FILE_PATH")
}
