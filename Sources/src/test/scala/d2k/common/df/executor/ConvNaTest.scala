package d2k.common.df.executor

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.common.TestArgs
import d2k.common.InputArgs

import spark.common.SparkContexts.context
import context.implicits._
import spark.common.SparkContexts

import d2k.common.df.template.DfToDf
import d2k.common.df.template.DfToDb
import org.apache.spark.sql.SaveMode
import spark.common.DbCtl

case class ConvNaData(ts: String, dt: String, str: String)
case class Sp01(DT: String, TSTMP: String, VC: String, CH: String)
class ConvNaTest extends WordSpec with MustMatchers with BeforeAndAfter {

  implicit val inArgs = TestArgs().toInputArgs
  import SparkContexts.context.implicits._

  "Date" should {
    "spaceが正しく変換される" in {
      val data = Seq(ConvNaData("", "", "")).toDF
      val target = new DfToDf with ConvNaDate {
        val componentId = "test"
        val dateColumns = Seq("dt")
      }

      val r = target.run(data).as[ConvNaData].collect.head
      r.ts mustBe ""
      r.dt mustBe "0001-01-01"
      r.str mustBe ""
    }

    "nullが正しく変換される" in {
      val data = Seq(ConvNaData(null, null, null)).toDF
      val target = new DfToDf with ConvNaDate {
        val componentId = "test"
        val dateColumns = Seq("dt")
      }

      val r = target.run(data).as[ConvNaData].collect.head
      r.ts mustBe null
      r.dt mustBe "0001-01-01"
      r.str mustBe null
    }
  }

  "Timestamp" should {
    "spaceが正しく変換される" in {
      val data = Seq(ConvNaData("", "", "")).toDF
      val target = new DfToDf with ConvNaTs {
        val componentId = "test"
        val tsColumns = Seq("ts")
      }

      val r = target.run(data).as[ConvNaData].collect.head
      r.ts mustBe "0001-01-01 00:00:00"
      r.dt mustBe ""
      r.str mustBe ""
    }

    "nullが正しく変換される" in {
      val data = Seq(ConvNaData(null, null, null)).toDF
      val target = new DfToDf with ConvNaTs {
        val componentId = "test"
        val tsColumns = Seq("ts")
      }

      val r = target.run(data).as[ConvNaData].collect.head
      r.ts mustBe "0001-01-01 00:00:00"
      r.dt mustBe null
      r.str mustBe null
    }
  }

  "Date and Timestamp" should {
    "spaceが正しく変換される" in {
      val data = Seq(ConvNaData("", "", "")).toDF
      val target = new DfToDf with ConvNa {
        val componentId = "test"
        val tsColumns = Seq("ts")
        val dateColumns = Seq("dt")
      }

      val r = target.run(data).as[ConvNaData].collect.head
      r.ts mustBe "0001-01-01 00:00:00"
      r.dt mustBe "0001-01-01"
      r.str mustBe ""
    }

    "nullが正しく変換される" in {
      val data = Seq(ConvNaData(null, null, null)).toDF
      val target = new DfToDf with ConvNa {
        val componentId = "test"
        val tsColumns = Seq("ts")
        val dateColumns = Seq("dt")
      }

      val r = target.run(data).as[ConvNaData].collect.head
      r.ts mustBe "0001-01-01 00:00:00"
      r.dt mustBe "0001-01-01"
      r.str mustBe null
    }
  }

  val dbCtl = new DbCtl()
  import dbCtl.implicits._

  "Db Write" should {
    "正しくDBに書き込まれる" in {
      val data = Seq(Sp01("", "", "", ""), Sp01(null, null, null, null)).toDF
      val target = new DfToDb with ConvNa {
        val componentId = "test"
        val tsColumns = Seq("TSTMP")
        val dateColumns = Seq("DT")

        override lazy val writeTableName = "sp01"
        override val writeDbSaveMode = SaveMode.Overwrite
        override val writeDbWithCommonColumn = false
      }

      target.run(data)

      val result = dbCtl.readTable("sp01").as[Sp01].collect

      {
        val r = result(0)
        r.DT mustBe "0001-01-01 00:00:00"
        r.TSTMP mustBe "0001-01-01 00:00:00"
        r.VC mustBe null
        r.CH mustBe null
      }

      {
        val r = result(1)
        r.DT mustBe "0001-01-01 00:00:00"
        r.TSTMP mustBe "0001-01-01 00:00:00"
        r.VC mustBe null
        r.CH mustBe null
      }
    }
  }
}
