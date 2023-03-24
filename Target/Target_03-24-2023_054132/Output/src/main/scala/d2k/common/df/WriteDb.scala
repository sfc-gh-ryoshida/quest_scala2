package d2k.common.df

import com.snowflake.snowpark.SaveMode
import spark.common.DbInfo
import com.snowflake.snowpark.DataFrame
import d2k.common.InputArgs
import spark.common.DbCtl
import com.snowflake.snowpark.functions._
import d2k.common.ResourceInfo
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
sealed trait WriteDbMode
 object WriteDbMode {
   /**
   * Insert用 writeDbSaveMode(Default:SaveMode.Append)とセットで利用する
   */
   case object Insert extends WriteDbMode

   /**
   * Insert(高速版)用 writeDbSaveMode(Default:SaveMode.Append)とセットで利用する
   */
   case object InsertAcc extends WriteDbMode

   /**
   * Insert not exist用 writeDbSaveMode(Default:SaveMode.Append)とセットで利用する
   */
   case class InsertNotExists (keyNames: String*) extends WriteDbMode

   /**
   * Update用 writeDbPrimaryKeysとセットで利用する
   */
   case object Update extends WriteDbMode

   /**
   * Upsert用 writeDbPrimaryKeysとセットで利用する
   */
   case object Upsert extends WriteDbMode

   /**
   * 論理削除用 writeDbPrimaryKeysとセットで利用する<br>
   * FG_D2KDELFLGへ"1"を設定しUpdateを行う
   */
   case object DeleteLogical extends WriteDbMode

   /**
   * 物理削除用 writeDbPrimaryKeysとセットで利用する
   */
   case object DeletePhysical extends WriteDbMode
}
trait WriteDb extends ResourceInfo {
   import WriteDbMode._

   lazy val writeTableName: String = componentId

   /**
   * DB書込みモード<br>
   * Insert, InsertAcc, Update, Upsert, DeletePhisical, DeleteLogical
   */
   val writeDbMode: WriteDbMode = Insert

   val writeDbSaveMode: SaveMode = SaveMode.Append

   val writeDbUpdateKeys: Set[String] = Set.empty[String]

   val writeDbUpdateIgnoreColumns: Set[String] = Set.empty[String]

   val writeDbHint: String = ""

   /**
   * 共通項目を付加するか
   * ture(default):付加する
   * false:付加しない
   */
   val writeDbWithCommonColumn: Boolean = true

   /**
   * DB情報の設定
   */
   val writeDbInfo: DbInfo = DbCtl.dbInfo1

   /**
   *  NA(空文字若しくはnull)の項目を" "space1文字へ置き換える
   *  true:置き換える false:置き換えない　defaultはfalse(置き換えない)
   */
   val writeDbConvNaMode: Boolean = false

   def writeDb(dforg: DataFrame)(implicit inArgs: InputArgs) = {
   val df = convNa(dforg)
 val tblName = inArgs.tableNameMapper.get(componentId).getOrElse(writeTableName)
 val dbCtl = new DbCtl (writeDbInfo)
 def modUpdateColumn(df: DataFrame) = df.withColumn("dt_d2kupddttm", lit(inArgs.sysSQLDate)).withColumn("id_d2kupdusr", lit(componentId))
 def checkKeys = if (writeDbUpdateKeys.isEmpty)
         throw new IllegalArgumentException ("writeDbUpdateKeys is empty") (writeDbMode, writeDbWithCommonColumn) match {
            case (Insert, true) => {
            dbCtl.insertAccelerated(DbCommonColumnAppender(df, componentId), tblName, writeDbSaveMode, writeDbHint)
            }
            case (Insert, false) => {
            dbCtl.insertAccelerated(df, tblName, writeDbSaveMode, writeDbHint)
            }
            case (InsertAcc, true) => {
            dbCtl.insertAccelerated(DbCommonColumnAppender(df, componentId), tblName, writeDbSaveMode, writeDbHint)
            }
            case (InsertAcc, false) => {
            dbCtl.insertAccelerated(df, tblName, writeDbSaveMode, writeDbHint)
            }
            case (InsertNotExists(keys@_*), true) => {
            dbCtl.insertNotExists(DbCommonColumnAppender(df, componentId), tblName, keys, writeDbSaveMode, writeDbHint)
            }
            case (InsertNotExists(keys@_*), false) => {
            dbCtl.insertNotExists(df, tblName, keys, writeDbSaveMode, writeDbHint)
            }
            case (Update, true) => {
            checkKeys
dbCtl.updateRecords(modUpdateColumn(df), tblName, writeDbUpdateKeys, writeDbUpdateIgnoreColumns, writeDbHint)
            }
            case (Update, false) => {
            checkKeys
dbCtl.updateRecords(df, tblName, writeDbUpdateKeys, writeDbUpdateIgnoreColumns, writeDbHint)
            }
            case (Upsert, true) => {
            checkKeys
dbCtl.upsertRecords(DbCommonColumnAppender(df, componentId), tblName, 
writeDbUpdateKeys, Set("dt_d2kmkdttm", "id_d2kmkusr", "nm_d2kupdtms", "fg_d2kdelflg") ++ writeDbUpdateIgnoreColumns, writeDbHint)
            }
            case (Upsert, false) => {
            checkKeys
dbCtl.upsertRecords(df, tblName, writeDbUpdateKeys, writeDbUpdateIgnoreColumns, writeDbHint)
            }
            case (DeleteLogical, true) => {
            checkKeys
 val deleteFlagName = "fg_d2kdelflg"
 val deleteTarget = df.withColumn(deleteFlagName, lit("1")).select(deleteFlagName, writeDbUpdateKeys.toSeq :_*)
dbCtl.updateRecords(
modUpdateColumn(deleteTarget), 
tblName, writeDbUpdateKeys, Set("dt_d2kmkdttm", "id_d2kmkusr", "nm_d2kupdtms"), writeDbHint)
            }
            case (DeleteLogical, false) => throw new IllegalArgumentException ("DeleteLogical and writeDbWithCommonColumn == false can not used be togather")
            case (DeletePhysical, _) => {
            checkKeys
dbCtl.deleteRecords(df, tblName, writeDbUpdateKeys, writeDbHint)
            }
         }
dforg.sqlContext.emptyDataFrame
   }

   def convNa(df: DataFrame) = if (writeDbConvNaMode)
      {
      df.na.fill(" ").na.replace(df.columns, Map("" -> " "))
      }
else
      {
      df
      }
}