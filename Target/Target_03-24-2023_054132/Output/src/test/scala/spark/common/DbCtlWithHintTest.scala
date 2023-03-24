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
import java.util.Properties
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object DbCtlWithHintTest {
   case class Data (key: String, data: String)
}
 class DbCtlWithHintTest extends WordSpec with MustMatchers with BeforeAndAfter {
   import DbCtlWithHintTest._

   import com.snowflake.snowpark.types._

   val currentTime = System.currentTimeMillis

   val dbtarget = DbCtl.dbInfo1

   "with hint" should {
   val tableName = "hinttest"
 val dbCtl = new DbCtl (dbtarget)
"insertAcc" in {
      dbCtl.insertAccelerated(Seq(Data("key", "d")).toDF, tableName, SaveMode.Overwrite, "TEST_HINT_INSERTACC")
      }
"insertNotExists" in {
      dbCtl.insertNotExists(Seq(Data("key", "d")).toDF, tableName, Seq("key"), SaveMode.Overwrite, "TEST_HINT_NOT_EXISTS")
      }
"insertDirectPathInsert" in {
      val dbCtl = new DbCtl (dbtarget.copy(isDirectPathInsertMode = true))
dbCtl.insertAccelerated(Seq(Data("key", "d")).toDF, tableName, SaveMode.Overwrite, "TEST_HINT_DIRECTPATH")
      }
"delete" in {
      val df = Seq(Data("key", "d")).toDF
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
dbCtl.deleteRecords(df.select("key"), tableName, Set("key"), "TEST_HINT_DELETE")
      }
"update" in {
      val df = Seq(Data("key", "d")).toDF
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
dbCtl.updateRecords(df.withColumn("data", lit("d2")), tableName, Set("key"), Set.empty, "TEST_HINT_UPDATE")
      }
"upsert" in {
      val df = Seq(Data("key", "d")).toDF
dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
 val df2 = Seq(Data("key", "dx"), Data("key2", "d2")).toDF
dbCtl.upsertRecords(df2, tableName, Set("key"), Set.empty, "TEST_HINT_UPSERT")
      }
   }
}