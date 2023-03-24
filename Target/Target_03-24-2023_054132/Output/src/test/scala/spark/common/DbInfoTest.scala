package spark.common

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import com.snowflake.snowpark.SaveMode
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class DbInfoTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "from env read" should {
   "be success" in {
      println(DbCtl.dbInfo1.commitSize)
println(DbCtl.dbInfo1.isDirectPathInsertMode)
      }
   }

   "prop test" should {
   val df = SparkContexts.context.emptyDataFrame
 val tableName = "largeData"
"set env dpi" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1)
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
"set source dpi" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(isDirectPathInsertMode = false))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
"set source and prop" in {
      System.setProperty("DPI_MODE", "true")
 val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(commitSize = Some(1000), isDirectPathInsertMode = false))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
"set env commitSize" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(isDirectPathInsertMode = false))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
"set source commitSize" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(commitSize = Some(1000), isDirectPathInsertMode = false))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
"set source and prop commitSize" in {
      System.setProperty("COMMIT_SIZE", "9999")
 val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(commitSize = Some(1000), isDirectPathInsertMode = false))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
"set source fetchSize" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(fetchSize = Some(1000)))
dbCtl.readTable(tableName)
      }
"set source and prop fetchSize" in {
      System.setProperty("FETCH_SIZE", "10000")
 val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(fetchSize = Some(1000)))
dbCtl.readTable(tableName)
      }
   }
}