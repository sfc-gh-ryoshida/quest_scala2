package org.apache.spark.sql

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts

/*EWI: SPRKSCL1142 => org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD is not supported*/
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD
import com.snowflake.snowpark.types.StructType

/*EWI: SPRKSCL1142 => org.apache.spark.sql.types.StructField is not supported*/
import org.apache.spark.sql.types.StructField
import com.snowflake.snowpark.types.StringType
import spark.common.DbCtl

/*EWI: SPRKSCL1142 => org.apache.spark.sql.execution.datasources.jdbc is not supported*/
import org.apache.spark.sql.execution.datasources.jdbc._

/*EWI: SPRKSCL1142 => org.apache.spark.sql.sources.Filter is not supported*/
import org.apache.spark.sql.sources.Filter
import java.sql.Timestamp

/*EWI: SPRKSCL1142 => org.apache.spark.sql.catalyst.InternalRow is not supported*/
import org.apache.spark.sql.catalyst.InternalRow

/*EWI: SPRKSCL1142 => org.apache.spark.sql.types.DecimalType is not supported*/
import org.apache.spark.sql.types.DecimalType

/*EWI: SPRKSCL1142 => org.apache.spark.sql.types.TimestampType is not supported*/
import org.apache.spark.sql.types.TimestampType

/*EWI: SPRKSCL1142 => org.apache.spark.sql.sources is not supported*/
import org.apache.spark.sql.sources._
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class JdbcCtlTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "JDBCRDD" should {
   "success connect db" in {
      val tableName = "sp02"
 val columns = Array("DT", "NUM5", "NUM52", "TSTMP", "VC", "CH")
 val dbCtl = new DbCtl ()
 val result = JdbcCtl.readTable(dbCtl, tableName, columns).collect

         /*EWI: SPRKSCL1142 => org.apache.spark.sql.JdbcCtlTest.result is not supported*/
         result.size mustBe 4

         /*EWI: SPRKSCL1142 => org.apache.spark.sql.JdbcCtlTest.result is not supported*/
         result.head.schema.size mustBe 6

         /*EWI: SPRKSCL1142 => org.apache.spark.sql.JdbcCtlTest.result is not supported*/
         val names = result.head.schema.map(_.name)
names.exists(_ == "DT") mustBe true
names.exists(_ == "NUM5") mustBe true
names.exists(_ == "NUM52") mustBe true
names.exists(_ == "TSTMP") mustBe true
names.exists(_ == "VC") mustBe true
names.exists(_ == "CH") mustBe true
      }
"success connect db select columns" in {
      val tableName = "sp02"
 val columns = Array("DT", "VC", "CH")
 val dbCtl = new DbCtl ()
 val result = JdbcCtl.readTable(dbCtl, tableName, columns).collect

         /*EWI: SPRKSCL1142 => org.apache.spark.sql.JdbcCtlTest.result is not supported*/
         result.size mustBe 4

         /*EWI: SPRKSCL1142 => org.apache.spark.sql.JdbcCtlTest.result is not supported*/
         result.head.schema.size mustBe 3

         /*EWI: SPRKSCL1142 => org.apache.spark.sql.JdbcCtlTest.result is not supported*/
         val names = result.head.schema.map(_.name)
names.exists(_ == "DT") mustBe true
names.exists(_ == "NUM5") mustBe false
names.exists(_ == "NUM52") mustBe false
names.exists(_ == "TSTMP") mustBe false
names.exists(_ == "VC") mustBe true
names.exists(_ == "CH") mustBe true
      }
"success connect db limit condition by where" in {
      val tableName = "sp02"
 val columns = Array("DT", "NUM5", "VC", "CH")
 val where = Array("NUM5 = '1000'", "NUM5 = '2000'")
 val dbCtl = new DbCtl ()
 val result = JdbcCtl.readTable(dbCtl, tableName, columns, where).collect

         /*EWI: SPRKSCL1142 => org.apache.spark.sql.JdbcCtlTest.result is not supported*/
         result.size mustBe 2
result(0).getAs[java.math.BigDecimal]("NUM5").toString mustBe "1000"
result(1).getAs[java.math.BigDecimal]("NUM5").toString mustBe "2000"
      }
"success connect db limit condition by filter" in {
      val tableName = "sp02"
 val columns = Array("DT", "NUM5", "NUM52", "TSTMP", "VC", "CH")
 val where = Array("NUM5 = '1000'", "NUM5 = '2000'")
 val filter: Array[Filter] = Array(EqualTo("NUM52", "10.3"))
 val dbCtl = new DbCtl ()
 val result = JdbcCtl.readTable(dbCtl, tableName, columns, where, filter).collect

         /*EWI: SPRKSCL1142 => org.apache.spark.sql.JdbcCtlTest.result is not supported*/
         result.size mustBe 1
result(0).getAs[java.math.BigDecimal]("NUM5").toString mustBe "1000"
result(0).getAs[java.math.BigDecimal]("NUM52").toString mustBe "10.30"
      }
   }
}