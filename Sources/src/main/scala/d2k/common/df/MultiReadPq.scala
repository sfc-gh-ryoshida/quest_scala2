package d2k.common.df

import spark.common.DbInfo
import spark.common.PqCtl
import d2k.common.InputArgs
import d2k.common.ResourceInfo

trait MultiReadPq extends ReadPq {
  val readPqNames: Seq[String]

  def readParquet(implicit inArgs: InputArgs) =
    readPqNames.map(pqName => (pqName, readParquetSingle(pqName))).toMap
}