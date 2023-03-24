package d2k.common.df

import spark.common.DbInfo
import d2k.common.InputArgs
import spark.common.DbCtl
import d2k.common.ResourceInfo

trait MultiReadDb extends ReadDb {
  val readTableNames: Seq[String]

  def readDb(implicit inArgs: InputArgs) = readTableNames.map(tblnm => (tblnm, readDbSingle(tblnm))).toMap
}