package spark.common

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts.context
import context.implicits._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.Row
import com.snowflake.snowpark.SaveMode
import scala.util.Try
import org.joda.time.DateTime
import java.sql.Timestamp
import java.sql.Date
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat

/*EWI: SPRKSCL1142 => org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils is not supported*/
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class LargeInsertTest extends WordSpec with MustMatchers with BeforeAndAfter {
   import com.snowflake.snowpark.types._

   val dbCtl = new DbCtl (DbCtl.dbInfo1)

   val tableColumns = (1 to 10).map( cnt =>s"col${ cnt } varchar(10)").mkString(",")

   Try{dbCtl.execSql("largeData", s"create table largeData($tableColumns) logging")}

   Try{dbCtl.execSql("largeDataNoLogging", s"create table largeDataNoLogging($tableColumns) nologging")}

   val structTypeAnyType = StructType((1 to 10).map( cnt =>StructField(s"col${ cnt }", StringType)))

   def colData(rec: Int) = (1 to 10).map( cnt =>s"${ cnt }_${ rec }").toSeq

   val data = (1 to 10000).map( cnt =>Row(colData(cnt) :_*))

   val inDf = context.createDataFrame(SparkContexts.sc.makeRDD(data), structTypeAnyType).cache

   val pqCtl = new PqCtl ("test/large")

   import pqCtl.implicits._

   //(1 to 100).par.foreach(cnt => inDf.writeParquet(s"large/$cnt"))
   val df = pqCtl.readParquet("large/*")

   "dummy" should {
   val tableName = "largeData"
"insert recs" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(isDirectPathInsertMode = false))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
   }

   "nonDirectInsert" should {
   val tableName = "largeData"
"insert recs" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(isDirectPathInsertMode = false))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
"insert recs with commit size 1000" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(commitSize = Some(1000), isDirectPathInsertMode = false))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
"insert recs with commit size 10000" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(commitSize = Some(10000), isDirectPathInsertMode = false))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
"insert recs with commit size 100000" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(commitSize = Some(100000), isDirectPathInsertMode = false))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
   }

   "directInsert" should {
   val tableName = "largeDataNoLogging"
"insert recs" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(isDirectPathInsertMode = true))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
"insert recs with commit size 1000" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(commitSize = Some(1000), isDirectPathInsertMode = true))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
"insert recs with commit size 10000" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(commitSize = Some(10000), isDirectPathInsertMode = true))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
"insert recs with commit size 100000" in {
      val dbCtl = new DbCtl (DbCtl.dbInfo1.copy(commitSize = Some(100000), isDirectPathInsertMode = true))
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      }
   }
}