package d2k.common.df

import spark.common.DbInfo
import spark.common.PqCtl
import d2k.common.InputArgs
import d2k.common.ResourceInfo

trait SingleReadPq extends ReadPq {
  lazy val readPqName: String = componentId

  def readParquet(implicit inArgs: InputArgs) = readParquetSingle(readPqName)
}