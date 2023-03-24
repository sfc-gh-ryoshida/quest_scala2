package d2k.common

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import d2k.common.df.DbConnectionInfo
import spark.common.SparkContexts
import spark.common.DbCtl
import SparkContexts.context.implicits._
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Properties
import org.apache.spark.sql._
import d2k.common.df.template.DbToDb
import d2k.common.df.template.DbToDb
import scala.util.Try
import org.apache.spark.sql.functions._
import java.sql.Date
import d2k.common.df.executor.Nothing

case class LastUpdateTime(ID_TBLID: String, DT_FROMUPDYMDTM: Timestamp,
                          DT_TOUPDYMDTM: Timestamp, DT_UPDYMDTM: Timestamp)
case class T1(ID: String, TMSTMP: Timestamp)
case class T2(a: String, b: String)
case class T3(aa: String, bb: String)
class InputArgsTest extends WordSpec with MustMatchers with BeforeAndAfter {
  implicit val inArgs = TestArgs().toInputArgs

  "InputArgs" should {
    "normal end" in {
      inArgs.tableNameMapper.isEmpty mustBe true
    }

    "baseInputFilePath" in {
      inArgs.baseInputFilePath mustBe "test/dev/data/output"
    }

    "be success read runningdate" in {
      import inArgs.runningDate._

      MANG_DT mustBe "20151001"
      YST_DY mustBe "20150930"
      NXT_DT mustBe "20150305"
      BEF_MO mustBe "201502"
      BEF_MO_FRST_MTH_DT mustBe "20150201"
      BEF_MO_MTH_DT mustBe "20150228"
      CURR_MO mustBe "201503"
      CURR_MO_FRST_MTH_DT mustBe "20150301"
      CURR_MO_MTH_DT mustBe "20150331"
      NXT_MO mustBe "201504"
      NXT_MO_FRST_MTH_DT mustBe "20150401"
      NXT_MO_MTH_DT mustBe "20150430"
    }
  }

  "InputArgs　RunningEnv daily execute cnt" should {
    "be normal end" in {
      inArgs.runningEnv.DAILY_EXECUTE_CNT mustBe "999"
    }
  }

  def timestamp_yyyymmdd(yyyy: Int, mm: Int, dd: Int) =
    Timestamp.valueOf(LocalDateTime.of(yyyy, mm, dd, 0, 0, 0))

  implicit class MakeDate(str: String) {
    def toTm =
      timestamp_yyyymmdd(str.take(4).toInt, str.drop(4).take(2).toInt, str.drop(6).take(2).toInt)
  }

  "差分抽出時刻管理テーブルデータ取得" should {
    val readDbInfo = DbConnectionInfo.bat1
    val dbCtl = new DbCtl(readDbInfo)
    Try { dbCtl.dropTable("MOP012") }
    println(readDbInfo.toOptions.tableOrQuery)
    JdbcUtils.createConnectionFactory(readDbInfo.toOptions)()
      .prepareStatement(
        """create table MOP012(
          |  ID_TBLID VARCHAR2(6),
          |  DT_FROMUPDYMDTM TIMESTAMP,
          |  DT_TOUPDYMDTM TIMESTAMP,
          |  DT_UPDYMDTM TIMESTAMP)
          |""".stripMargin).executeUpdate

    import dbCtl.implicits._

    "be success lastUpdateTime." in {
      Seq(
        LastUpdateTime("X1", "20000101".toTm, "20010101".toTm, "20170101".toTm),
        LastUpdateTime("X2", "20010102".toTm, "20020102".toTm, "20170102".toTm)).toDF.writeTable("MOP012")

      val x1 = inArgs.lastUpdateTime("X1")
      x1.from mustBe "20000101".toTm
      x1.to mustBe "20010101".toTm

      val x2 = inArgs.lastUpdateTime("X2")
      x2.from mustBe "20010102".toTm
      x2.to mustBe "20020102".toTm
    }

    "tableNameが未定義の場合IllegalArgumentExceptionがthrowされる" in {
      Seq(
        LastUpdateTime("X1", "20000101".toTm, "20010101".toTm, "20170101".toTm),
        LastUpdateTime("X2", "20010102".toTm, "20020102".toTm, "20170102".toTm)).toDF.writeTable("MOP012")

      try {
        inArgs.lastUpdateTime("othertable")
        fail
      } catch {
        case t: Throwable => t.getClass.getName mustBe "java.lang.IllegalArgumentException"
      }
    }

    "template実装 sample" in {
      Try { dbCtl.dropTable("T1") }
      JdbcUtils.createConnectionFactory(readDbInfo.toOptions)()
        .prepareStatement(
          """create table T1(
            |  ID VARCHAR2(2),
            |  TMSTMP TIMESTAMP)
            |""".stripMargin).executeUpdate

      Try { dbCtl.dropTable("X2") }
      JdbcUtils.createConnectionFactory(readDbInfo.toOptions)()
        .prepareStatement(
          """create table X2(
            |  ID VARCHAR2(2),
            |  TMSTMP TIMESTAMP)
            |""".stripMargin).executeUpdate

      (1 to 10).map { idx =>
        T1(idx.toString, f"200101${idx}%02d".toTm)
      }.toDF.writeTable("T1")

      Seq(
        LastUpdateTime("X1", "20010101".toTm, "20010101".toTm, "20170101".toTm),
        LastUpdateTime("X2", "20010103".toTm, "20010105".toTm, "20170102".toTm)).toDF.writeTable("MOP012", SaveMode.Overwrite)

      object LastUpdateTimeApp extends SparkApp {
        def exec(implicit inArgs: InputArgs) = {
          dbTodb.run(Unit)
        }

        val dbTodb = new DbToDb with Nothing {
          val componentId = "test1"
          override lazy val readTableName = "T1"
          override def readDbWhere(inArgs: InputArgs) = {
            val ut = inArgs.lastUpdateTime(writeTableName)
            Array(s"TMSTMP >= '${ut.from}' and TMSTMP <= '${ut.to}'")
          }

          override lazy val writeTableName = "X2"
          override val writeDbWithCommonColumn = false
        }
      }

      LastUpdateTimeApp.exec

      val result = dbCtl.readTable("X2").as[T1].collect
      result.size mustBe 3

      result(0).ID mustBe "3"
      result(1).ID mustBe "4"
      result(2).ID mustBe "5"
    }

    "template実装 tableName未定義" in {
      object LastUpdateTimeApp2 extends SparkApp {
        def exec(implicit inArgs: InputArgs) = {
          dbTodb.run(Unit)
        }

        val dbTodb = new DbToDb with Nothing {
          val componentId = "test"
          override lazy val readTableName = "T1"
          override def readDbWhere(inArgs: InputArgs) =
            Array(s"TMSTMP >= '${inArgs.lastUpdateTime("xxx")}'")
        }
      }

      try {
        LastUpdateTimeApp2.exec
        fail
      } catch {
        case t: Throwable => t.getClass.getName mustBe "java.lang.IllegalArgumentException"
      }
    }

    "InputArgs can be Broadcasting" when {
      "join" in {
        val inputArgs: InputArgs = TestArgs().toInputArgs
        val data1 = (1 to 100).map(cnt => T2(s"a${cnt}", s"b${cnt}")).toDF
        val data2 = (1 to 100).map(cnt => T3(s"a${cnt}", s"bb${cnt}")).toDF

        data1.join(data2, $"a" === $"aa").withColumn("b", lit(inputArgs.runningDate.MANG_DT)).show
        succeed
      }

      "repartition" in {
        val inputArgs: InputArgs = TestArgs().toInputArgs
        val data = (1 to 100).map(cnt => T2(s"a${cnt}", s"b${cnt}")).toDF

        data.repartition(10).withColumn("b", lit(inputArgs.runningDate.MANG_DT)).show
        succeed
      }
    }
  }
}
