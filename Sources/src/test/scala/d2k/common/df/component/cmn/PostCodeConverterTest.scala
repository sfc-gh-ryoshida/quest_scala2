package d2k.common.df.component.cmn

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter

import spark.common.SparkContexts
import SparkContexts.context.implicits._
import spark.common.DbCtl
import spark.common.DfCtl.implicits._
import d2k.common.df.DbConnectionInfo
import d2k.common.TestArgs
import org.apache.spark.sql.DataFrame
import scala.util.Try
import spark.common.PqCtl

case class PostCodeConverterTestData(CD_POST: String, CD_POST2: String = "")
case class PostCodeInputData(CD_ZIP7LEN: String, CD_KENCD: String, CD_DEMEGRPCD: String)
class PostCodeConverterTest extends WordSpec with MustMatchers with BeforeAndAfter {
  implicit val inArgs = TestArgs().toInputArgs

  val pqDf = Seq(
    PostCodeInputData("0010010", "01", "101"),
    PostCodeInputData("00200  ", "01", "102"),
    PostCodeInputData("003    ", "01", "103"),
    PostCodeInputData("0040000", "01", "104"),
    PostCodeInputData("004    ", "02", "204"),
    PostCodeInputData("005    ", "01", "105"),
    PostCodeInputData("0060010", "", ""),
    PostCodeInputData("0070010", null, null)).toDF
  pqDf.write.mode("overwrite").parquet("test/dev/data/output/MBA935.pq")

  "PostCodeConverter" should {
    "normal end" when {
      "7桁パターン" when {
        val df = Seq(PostCodeConverterTestData("0010010")).toDF.cache
        "県コードのみ出力" in {
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
          result.schema.fields.size mustBe 3
          result.getAs[String]("CD_KENCD") mustBe "01"
        }

        "県コード/市区町村両方出力" in {
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result.schema.fields.size mustBe 4
          result.getAs[String]("CD_KENCD") mustBe "01"
          result.getAs[String]("CD_DEMEGRPCD") mustBe "101"
        }

        "複数回呼出" in {
          val pccnv = PostCodeConverter()

          val result = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
          result.schema.fields.size mustBe 3
          result.getAs[String]("CD_KENCD") mustBe "01"

          val result2 = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result2.schema.fields.size mustBe 4
          result2.getAs[String]("CD_KENCD") mustBe "01"
          result2.getAs[String]("CD_DEMEGRPCD") mustBe "101"
        }
      }

      "5桁パターン" when {
        val df = Seq(PostCodeConverterTestData("00200  ")).toDF.cache
        "複数回呼出" in {
          val pccnv = PostCodeConverter()

          val result = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
          result.schema.fields.size mustBe 3
          result.getAs[String]("CD_KENCD") mustBe "01"

          val result2 = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result2.schema.fields.size mustBe 4
          result2.getAs[String]("CD_KENCD") mustBe "01"
          result2.getAs[String]("CD_DEMEGRPCD") mustBe "102"
        }
      }

      "3桁パターン" when {
        val df = Seq(PostCodeConverterTestData("003    ")).toDF.cache
        "複数回呼出" in {
          val pccnv = PostCodeConverter()

          val result = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
          result.schema.fields.size mustBe 3
          result.getAs[String]("CD_KENCD") mustBe "01"

          val result2 = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result2.schema.fields.size mustBe 4
          result2.getAs[String]("CD_KENCD") mustBe "01"
          result2.getAs[String]("CD_DEMEGRPCD") mustBe "103"
        }
      }

      "8桁パターン" when {
        val df = Seq(PostCodeConverterTestData("001-0010  ")).toDF.cache
        "複数回呼出" in {
          val pccnv = PostCodeConverter()

          val result = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
          result.schema.fields.size mustBe 3
          result.getAs[String]("CD_KENCD") mustBe "01"

          val result2 = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result2.schema.fields.size mustBe 4
          result2.getAs[String]("CD_KENCD") mustBe "01"
          result2.getAs[String]("CD_DEMEGRPCD") mustBe "101"
        }
      }

      "親子分割パターン" when {
        val df = Seq(PostCodeConverterTestData("001  ", "0010  ")).toDF.cache
        "複数回呼出" in {
          val pccnv = PostCodeConverter()

          val result = (df ~> pccnv.localGovernmentCode("CD_POST", "CD_POST2")("CD_KENCD")).collect.apply(0)
          result.schema.fields.size mustBe 3
          result.getAs[String]("CD_KENCD") mustBe "01"

          val result2 = (df ~> pccnv.localGovernmentCode("CD_POST", "CD_POST2")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result2.schema.fields.size mustBe 4
          result2.getAs[String]("CD_KENCD") mustBe "01"
          result2.getAs[String]("CD_DEMEGRPCD") mustBe "101"
        }
      }
    }

    "next match" when {
      "7桁パターン" when {
        "0000パターンでマッチ" in {
          val df = Seq(PostCodeConverterTestData("0049999")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result.schema.fields.size mustBe 4
          result.getAs[String]("CD_KENCD") mustBe "01"
          result.getAs[String]("CD_DEMEGRPCD") mustBe "104"
        }

        "スペースパターンでマッチ" in {
          val df = Seq(PostCodeConverterTestData("0059999")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result.schema.fields.size mustBe 4
          result.getAs[String]("CD_KENCD") mustBe "01"
          result.getAs[String]("CD_DEMEGRPCD") mustBe "105"
        }
      }

      "8桁パターン" when {
        "0000パターンでマッチ" in {
          val df = Seq(PostCodeConverterTestData("004-9999")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result.schema.fields.size mustBe 4
          result.getAs[String]("CD_KENCD") mustBe "01"
          result.getAs[String]("CD_DEMEGRPCD") mustBe "104"
        }

        "スペースパターンでマッチ" in {
          val df = Seq(PostCodeConverterTestData("005-9999")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result.schema.fields.size mustBe 4
          result.getAs[String]("CD_KENCD") mustBe "01"
          result.getAs[String]("CD_DEMEGRPCD") mustBe "105"
        }
      }

      "5桁パターン" when {
        "スペースパターンでマッチ" in {
          val df = Seq(PostCodeConverterTestData("00499")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result.schema.fields.size mustBe 4
          result.getAs[String]("CD_KENCD") mustBe "02"
          result.getAs[String]("CD_DEMEGRPCD") mustBe "204"
        }
      }

      "3桁パターン" when {
        "スペースパターンでマッチ" in {
          val df = Seq(PostCodeConverterTestData("004")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result.schema.fields.size mustBe 4
          result.getAs[String]("CD_KENCD") mustBe "02"
          result.getAs[String]("CD_DEMEGRPCD") mustBe "204"
        }
      }
    }

    "unmatched end" when {
      "7桁パターン" when {
        "県コードのみ出力" in {
          val df = Seq(PostCodeConverterTestData("0069999")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
          result.schema.fields.size mustBe 3
          result.getAs[String]("CD_KENCD") mustBe "99"
        }

        "県コード/市区町村両方出力" in {
          val df = Seq(PostCodeConverterTestData("0069999")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result.schema.fields.size mustBe 4
          result.getAs[String]("CD_KENCD") mustBe "99"
          result.getAs[String]("CD_DEMEGRPCD") mustBe "999"
        }
      }

      "8桁パターン" when {
        "県コードのみ出力" in {
          val df = Seq(PostCodeConverterTestData("006-9999")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
          result.schema.fields.size mustBe 3
          result.getAs[String]("CD_KENCD") mustBe "99"
        }

        "県コード/市区町村両方出力" in {
          val df = Seq(PostCodeConverterTestData("006-9999")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result.schema.fields.size mustBe 4
          result.getAs[String]("CD_KENCD") mustBe "99"
          result.getAs[String]("CD_DEMEGRPCD") mustBe "999"
        }
      }

      "5桁パターン" when {
        "県コードのみ出力" in {
          val df = Seq(PostCodeConverterTestData("00699")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
          result.schema.fields.size mustBe 3
          result.getAs[String]("CD_KENCD") mustBe "99"
        }

        "県コード/市区町村両方出力" in {
          val df = Seq(PostCodeConverterTestData("00699")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result.schema.fields.size mustBe 4
          result.getAs[String]("CD_KENCD") mustBe "99"
          result.getAs[String]("CD_DEMEGRPCD") mustBe "999"
        }
      }

      "3桁パターン" when {
        "県コードのみ出力" in {
          val df = Seq(PostCodeConverterTestData("006")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
          result.schema.fields.size mustBe 3
          result.getAs[String]("CD_KENCD") mustBe "99"
        }

        "県コード/市区町村両方出力" in {
          val df = Seq(PostCodeConverterTestData("006")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result.schema.fields.size mustBe 4
          result.getAs[String]("CD_KENCD") mustBe "99"
          result.getAs[String]("CD_DEMEGRPCD") mustBe "999"
        }
      }

      "3,5,7,8桁以外" when {
        "県コードのみ出力" in {
          val df = Seq(PostCodeConverterTestData("01")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
          result.schema.fields.size mustBe 3
          result.getAs[String]("CD_KENCD") mustBe "99"
        }

        "県コード/市区町村両方出力" in {
          val df = Seq(PostCodeConverterTestData("01")).toDF.cache
          val result = (df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
          result.schema.fields.size mustBe 4
          result.getAs[String]("CD_KENCD") mustBe "99"
          result.getAs[String]("CD_DEMEGRPCD") mustBe "999"
        }
      }
    }

    "abnormal data" when {
      "empty string" in {
        val df = Seq(PostCodeConverterTestData("0060010")).toDF.cache
        val pccnv = PostCodeConverter()

        val result = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
        result.schema.fields.size mustBe 3
        result.getAs[String]("CD_KENCD") mustBe "99"

        val result2 = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
        result2.schema.fields.size mustBe 4
        result2.getAs[String]("CD_KENCD") mustBe "99"
        result2.getAs[String]("CD_DEMEGRPCD") mustBe "999"
      }

      "null value" in {
        val df = Seq(PostCodeConverterTestData("0070010")).toDF.cache
        val pccnv = PostCodeConverter()

        val result = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
        result.schema.fields.size mustBe 3
        result.getAs[String]("CD_KENCD") mustBe "99"

        val result2 = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
        result2.schema.fields.size mustBe 4
        result2.getAs[String]("CD_KENCD") mustBe "99"
        result2.getAs[String]("CD_DEMEGRPCD") mustBe "999"
      }

      "input null value" in {
        val df = Seq(PostCodeConverterTestData(null)).toDF.cache
        val pccnv = PostCodeConverter()

        val result = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
        result.schema.fields.size mustBe 3
        result.getAs[String]("CD_KENCD") mustBe "99"

        val result2 = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
        result2.schema.fields.size mustBe 4
        result2.getAs[String]("CD_KENCD") mustBe "99"
        result2.getAs[String]("CD_DEMEGRPCD") mustBe "999"
      }

      "input null value. with child" in {
        val df = Seq(PostCodeConverterTestData(null, null)).toDF.cache
        val pccnv = PostCodeConverter()

        val result = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD")).collect.apply(0)
        result.schema.fields.size mustBe 3
        result.getAs[String]("CD_KENCD") mustBe "99"

        val result2 = (df ~> pccnv.localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")).collect.apply(0)
        result2.schema.fields.size mustBe 4
        result2.getAs[String]("CD_KENCD") mustBe "99"
        result2.getAs[String]("CD_DEMEGRPCD") mustBe "999"
      }
    }
  }
}
