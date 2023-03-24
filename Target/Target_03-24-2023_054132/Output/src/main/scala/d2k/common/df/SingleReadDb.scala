package d2k.common.df

import spark.common.DbInfo
import d2k.common.InputArgs
import spark.common.DbCtl
import d2k.common.ResourceInfo
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait SingleReadDb extends ReadDb {
   lazy val readTableName: String = componentId

   def readDb(implicit inArgs: InputArgs) = readDbSingle(readTableName)
}