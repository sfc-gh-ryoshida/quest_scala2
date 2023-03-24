package d2k.common.df

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts.context
import context.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import d2k.common.InputArgs
import org.apache.spark.sql.types._
import spark.common.SparkContexts
import spark.common.DbCtl
import org.apache.spark.sql.SaveMode
import java.sql.Timestamp
import WriteDbMode._
import scala.io.Source
import d2k.common.df.WriteFileMode._
import d2k.common.TestArgs
import spark.common.DbInfo
import java.sql.Date
import org.joda.time.DateTime
import java.sql.Timestamp
import d2k.common.ResourceInfo
import org.scalatest.Ignore
import d2k.common.df.executor.Nothing

class HiRDB_readTest extends WordSpec with MustMatchers with BeforeAndAfter {
  def d2s(dateMill: Long) = new DateTime(dateMill).toString("yyyy-MM-dd")
  def d2s(date: Date) = new DateTime(date).toString("yyyy-MM-dd")
  def d2s(date: Timestamp) = new DateTime(date).toString("yyyy-MM-dd hh:mm:ss")

  //テスト用DDL:　test/dev/conf/ddl/hirdb-make-schema.sql
  "HiRDB" should {
    implicit val inArgs = TestArgs().toInputArgs
    val structType = StructType(Seq(
      StructField("KEY", StringType), StructField("TEST", StringType)))
    val dbCtl = new DbCtl(DbConnectionInfo.hi1)
    import dbCtl.implicits._

    "success read table" in {
      val result = dbCtl.readTable("cs_test").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 1

      result(0).getAs[String]("KEY") mustBe "key1"
      result(0).getAs[String]("TEST") mustBe "aaa"
    }

    "success data check" in {
      val result = dbCtl.readTable("type_test").collect.head
      result.getAs[String]("KEY") mustBe "key1"
      result.getAs[String]("CHR") mustBe "chr  "
      result.getAs[String]("MVCHR") mustBe "xx漢字xx"
      result.getAs[Integer]("INT") mustBe 10000
      result.getAs[Integer]("SINT") mustBe 20000
      result.getAs[java.math.BigDecimal]("DCML") mustBe new java.math.BigDecimal("30.333")
      result.getAs[Float]("FLT") mustBe (400.444).toFloat
      result.getAs[Double]("SFLT") mustBe (5.0).toDouble
      result.getAs[Date]("DATE") mustBe DateTime.parse("2016-01-01").toDate
      result.getAs[Timestamp]("TMSTMP") mustBe new Timestamp(new DateTime(2016, 1, 1, 11, 22, 33).getMillis)
    }

    "success type check" in {
      val result = dbCtl.readTable("type_test")
      val fields = result.schema.fields

      val checkList = Seq(("KEY", "StringType"),
        ("CHR", "StringType"),
        ("VCHR", "StringType"),
        ("MCHR", "StringType"),
        ("INT", "IntegerType"),
        ("SINT", "IntegerType"),
        ("DCML", "DecimalType(5,3)"),
        ("FLT", "FloatType"),
        ("SFLT", "DoubleType"),
        ("DATE", "DateType"),
        ("TMSTMP", "TimestampType"))

      for {
        (name, t) <- checkList
        f <- fields.filter(_.name == name)
      } {
        f.dataType.toString mustBe t
      }
    }

    "success date check" in {
      val result = dbCtl.readTable("date_test").show
    }

    "success time check" in {
      try {
        val result = dbCtl.readTable("time_test").show
        fail
      } catch {
        case t: Throwable => t.printStackTrace()
      }
    }

    "success timestamp check" in {
      val result = dbCtl.readTable("tmstmp_test").show
    }

    "success insert" ignore {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq.empty[Row]), structType)
      val target = new WriteDb {
        val componentId = "cs_test"
      }
      val insertDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", "aaa"))), structType)
      target.writeDb(insertDf)

      dbCtl.readTable("cs_test").show
      val result = dbCtl.readTable("cs_test").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 1

      result(0).getAs[String]("TEST") mustBe "aaa"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "cs_test"
    }

    def dateAddHyphen(str: String) = s"${str.take(4)}-${str.drop(4).take(2)}-${str.drop(6)}"
    "success 'where' test for date" in {
      val fileToDf = new template.DbToDf with Nothing {
        val componentId = "where_test"
        override val readDbInfo = DbConnectionInfo.hi1
        override def readDbWhere(inArgs: InputArgs) = Array(s""""DATE" = '${dateAddHyphen(inArgs.runningDates(0))}'""")
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 1
      result.head.getAs[String]("KEY") mustBe "wheretest_date"
    }

    "success 'where' test for timestamp" in {
      val fileToDf = new template.DbToDf with Nothing {
        val componentId = "where_test"
        override val readDbInfo = DbConnectionInfo.hi1
        override def readDbWhere(inArgs: InputArgs) = Array(s""""TMSTMP" = '${dateAddHyphen(inArgs.runningDates(0))} 00:00:00'""")
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 1
      result.head.getAs[String]("KEY") mustBe "wheretest_timestamp"
    }
  }
}
