package d2k.common.df

import spark.common.DbInfo
import d2k.common.InputArgs
import spark.common.DbCtl
import d2k.common.ResourceInfo

object DbConnectionInfo {
  def mkDbInfo(envLabel: String) =
    DbInfo(sys.env(s"DB_URL_$envLabel"), sys.env(s"DB_USER_$envLabel"), sys.env(s"DB_PASSWORD_$envLabel"))
  lazy val dm1 = mkDbInfo("DM1")
  lazy val dwh1 = mkDbInfo("DWH1")
  lazy val dwh2 = mkDbInfo("DWH2")
  lazy val bat1 = mkDbInfo("BAT1")
  lazy val csp1 = mkDbInfo("CSP1")
  lazy val hi1 = mkDbInfo("HI1")
  lazy val mth1 = mkDbInfo("MTH1")
  lazy val fak1 = mkDbInfo("FAK1")
}

trait ReadDb extends ResourceInfo {
  /**
   * 読込Column選択
   */
  val columns: Array[String] = Array.empty[String]

  /**
   * DB読込時条件
   */
  val readDbWhere: Array[String] = Array.empty[String]
  def readDbWhere(inArgs: InputArgs): Array[String] = Array.empty[String]

  /**
   * DB情報の設定
   */
  val readDbInfo: DbInfo = DbConnectionInfo.bat1

  def readDbSingle(tableName: String)(implicit inArgs: InputArgs) = {
    val tblName = inArgs.tableNameMapper.get(componentId).getOrElse(tableName)
    val dbCtl = new DbCtl(readDbInfo)
    val readDbWhereWithArgs = readDbWhere(inArgs)
    (readDbWhere.isEmpty, readDbWhereWithArgs.isEmpty) match {
      case (true, true)   => selectReadTable(dbCtl, tblName)
      case (false, true)  => selectReadTable(dbCtl, tblName, readDbWhere)
      case (true, false)  => selectReadTable(dbCtl, tblName, readDbWhereWithArgs)
      case (false, false) => throw new IllegalArgumentException("Can not defined both readDbWhere and readDbWhere(inArgs)")
    }
  }

  def selectReadTable(dbCtl: DbCtl, tableName: String, readDbWhere: Array[String] = Array.empty[String]) = {
    (columns.isEmpty, readDbWhere.isEmpty) match {
      case (true, true)   => dbCtl.readTable(tableName)
      case (true, false)  => dbCtl.readTable(tableName, readDbWhere)
      case (false, true)  => dbCtl.readTable(tableName, columns, Array("1 = 1"))
      case (false, false) => dbCtl.readTable(tableName, columns, readDbWhere)
    }
  }
}
