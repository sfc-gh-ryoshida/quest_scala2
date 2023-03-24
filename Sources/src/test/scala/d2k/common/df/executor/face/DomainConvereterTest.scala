package d2k.common.df.executor.face

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts
import SparkContexts.context.implicits._
import spark.common.DbCtl
import d2k.common.df.DbConnectionInfo
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

case class Data(d01: String, d02: String, d03: String, d04: String,
                d05: String, d06: String, d07: String, d08: String,
                d09: String, d10: String)

import d2k.common.df.template.DfToDf
import d2k.common.TestArgs

class DomainConvereterTest extends WordSpec with MustMatchers with BeforeAndAfter {
  implicit val inArgs = TestArgs().toInputArgs

  "pattern1" should {
    val compo = new DfToDf with DomainConverter {
      val targetColumns = Set(
        ("d01", "年月日"), ("d02", "年月"), ("d03", "月日"), ("d04", "年"), ("d05", "月"),
        ("d06", "日"), ("d07", "年月日時分秒"), ("d08", "年月日時分"), ("d09", "年月日時分ミリ秒"), ("d10", "年月日時"))
    }

    "be success" when {
      "nomal data" in {
        val df = Seq(Data("20170101", "201702", "0201", "2018", "03", "31",
          "20170102030405", "201701030000", "20170104050607999", "2017010500")).toDF

        val result = compo.run(df).as[Data].collect.head

        result.toString mustBe Data("2017-01-01", "201702", "0201", "2018", "03", "31",
          "2017-01-02 03:04:05", "201701030000", "2017-01-04 05:06:07.999", "2017010500").toString
      }

      "nomal data2" in {
        val df = Seq(Data("00010102", "201702", "0201", "2018", "03", "31",
          "99991231235959", "201701030000", "99991231235959999", "2017010500")).toDF

        val result = compo.run(df).as[Data].collect.head

        result.toString mustBe Data("0001-01-02", "201702", "0201", "2018", "03", "31",
          "9999-12-31 23:59:59", "201701030000", "9999-12-31 23:59:59.999", "2017010500").toString
      }

      "invalid data" in {
        val df = Seq(Data("x", "x", "x", "x", "x", "x", "x", "x", "x", "x")).toDF

        val result = compo.run(df).as[Data].collect.head

        result.toString mustBe Data("0001-01-01", "000101", "0101", "0001", "01", "01",
          "0001-01-01 00:00:00", "000101010000", "0001-01-01 00:00:00", "0001010100").toString
      }

      "null data" in {
        val df = Seq(Data(null, null, null, null, null, null, null, null, null, null)).toDF

        val result = compo.run(df).as[Data].collect.head

        result.toString mustBe Data("0001-01-01", "000101", "0101", "0001", "01", "01",
          "0001-01-01 00:00:00", "000101010000", "0001-01-01 00:00:00", "0001010100").toString
      }
    }
  }

  "pattern2" should {
    val compo = new DfToDf with DomainConverter {
      val targetColumns = Set(
        ("d01", "時分秒"), ("d02", "時分ミリ秒"), ("d03", "時分"), ("d04", "時"), ("d05", "分"),
        ("d06", "秒"), ("d07", "時間"))
    }

    "be success" when {
      "nomal data" in {
        val df = Seq(Data("010101", "010102000", "0102", "03", "04", "05",
          "000006", "", "", "")).toDF

        val result = compo.run(df).as[Data].collect.head

        result.toString mustBe Data("010101", "010102000", "0102", "03", "04", "05",
          "000006", "", "", "").toString
      }

      "invalid data" in {
        val df = Seq(Data("x", "x", "x", "x", "x", "x", "x", "", "", "")).toDF

        val result = compo.run(df).as[Data].collect.head

        result.toString mustBe Data("000000", "000000000", "0000", "00", "00", "00",
          "000000", "", "", "").toString
      }

      "null data" in {
        val df = Seq(Data(null, null, null, null, null, null, null, null, null, null)).toDF

        val result = compo.run(df).as[Data].collect.head

        result.toString mustBe Data("000000", "000000000", "0000", "00", "00", "00",
          "000000", null, null, null).toString
      }
    }
  }

  "pattern3" should {
    val compo = new DfToDf with DomainConverter {
      val targetColumns = Set(
        ("d01", "文字列"), ("d02", "文字列_trim_半角"), ("d03", "文字列_trim_全角"), ("d04", "文字列_trim_全半角"), ("d05", "文字列_trim_無し"),
        ("d06", "全角文字列"), ("d07", "全角文字列_trim_全角"), ("d08", "全角文字列_trim_無し"), ("d09", "通信方式"))
    }

    "be success" when {
      "nomal data" in {
        val df = Seq(Data("x", " xx ", "　あ　", "　 xxx　 ", "　 xxxx　 ", "い",
          "　う　", "　え　", "   123  ", "")).toDF

        val result = compo.run(df).as[Data].collect.head

        result.toString mustBe Data("x", "xx", "あ", "xxx", "　 xxxx　 ", "い",
          "う", "　え　", "1", "").toString
      }
    }
  }
}
