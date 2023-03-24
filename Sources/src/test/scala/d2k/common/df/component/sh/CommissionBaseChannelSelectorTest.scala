package d2k.common.df.component.sh

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

object CommissionBaseChannelSelectorTest {
  case class Data(key: String, CD_CHNLGRPCD: String, CD_CHNLDETAILCD: String)
  implicit class ToData(t3: Seq[Tuple3[String, String, String]]) {
    def toDF = t3.map { case (a, b, c) => Data(a, b, c) }.toDF
  }
}
class CommissionBaseChannelSelectorTest extends WordSpec with MustMatchers with BeforeAndAfter {
  implicit val inArgs = TestArgs().toInputArgs
  import CommissionBaseChannelSelector._
  import CommissionBaseChannelSelectorTest._

  def check(key: String, DV_OUTOBJDIV: String, DV_TRICALCOBJDIV: String) = { (df: DataFrame) =>
    df.filter($"key" === key).collect.foreach { row =>
      row.getAs[String]("DV_OUTOBJDIV") mustBe DV_OUTOBJDIV
      row.getAs[String]("DV_TRICALCOBJDIV") mustBe DV_TRICALCOBJDIV
    }
    df
  }

  val df = Seq(
    ("a", "999", "999"),
    ("b", "600", "641"),
    ("c", "   ", "641"),
    ("d", "600", "   "),
    ("e", "   ", "   ")).toDF

  "comm試算" should {
    "normal end with key" when {
      val result = df ~> comm試算("key").run ~> ((_: DataFrame).cache)
      "N-01-01, N-01-02" in result ~> check("a", "1", "1")
      "N-02-01-01" in result ~> check("b", "0", "1")
      "N-02-01-02" in result ~> check("c", "0", "1")
      "N-02-01-03" in result ~> check("d", "1", "0")
      "N-02-01-04" in result ~> check("e", "1", "1")
    }

    "normal end without key" when {
      val result = df ~> comm試算.run ~> ((_: DataFrame).cache)
      "N-01-01, N-01-02" in result ~> check("a", "1", "1")
      "N-02-01-01" in result ~> check("b", "0", "1")
      "N-02-01-02" in result ~> check("c", "0", "1")
      "N-02-01-03" in result ~> check("d", "1", "0")
      "N-02-01-04" in result ~> check("e", "1", "1")
    }
  }

  "comm実績_月次手数料" should {
    "normal end with key" when {
      val result = df ~> comm実績_月次手数料("key").run ~> ((_: DataFrame).cache)
      "N-02-02-01" in result ~> check("b", "0", "2")
      "N-02-02-02" in result ~> check("c", "0", "2")
      "N-02-02-03" in result ~> check("d", "2", "0")
      "N-02-02-04" in result ~> check("e", "1", "1")
    }

    "normal end without key" when {
      val result = df ~> comm実績_月次手数料.run ~> ((_: DataFrame).cache)
      "N-02-02-01" in result ~> check("b", "0", "2")
      "N-02-02-02" in result ~> check("c", "0", "2")
      "N-02-02-03" in result ~> check("d", "2", "0")
      "N-02-02-04" in result ~> check("e", "1", "1")
    }
  }

  "comm実績_割賦充当" should {
    "normal end with key" when {
      val result = df ~> comm実績_割賦充当("key").run ~> ((_: DataFrame).cache)
      "N-02-03-01" in result ~> check("b", "0", "3")
      "N-02-03-02" in result ~> check("c", "0", "3")
      "N-02-03-03" in result ~> check("d", "3", "0")
      "N-02-03-04" in result ~> check("e", "1", "1")
    }

    "normal end without key" when {
      val result = df ~> comm実績_割賦充当.run ~> ((_: DataFrame).cache)
      "N-02-03-01" in result ~> check("b", "0", "3")
      "N-02-03-02" in result ~> check("c", "0", "3")
      "N-02-03-03" in result ~> check("d", "3", "0")
      "N-02-03-04" in result ~> check("e", "1", "1")
    }
  }

  "comm実績_直営店" should {
    "normal end with key" when {
      val result = df ~> comm実績_直営店("key").run ~> ((_: DataFrame).cache)
      "N-02-04-01" in result ~> check("b", "0", "4")
      "N-02-04-02" in result ~> check("c", "0", "4")
      "N-02-04-03" in result ~> check("d", "4", "0")
      "N-02-04-04" in result ~> check("e", "0", "0")
    }

    "normal end without key" when {
      val result = df ~> comm実績_直営店.run ~> ((_: DataFrame).cache)
      "N-02-04-01" in result ~> check("b", "0", "4")
      "N-02-04-02" in result ~> check("c", "0", "4")
      "N-02-04-03" in result ~> check("d", "4", "0")
      "N-02-04-04" in result ~> check("e", "0", "0")
    }
  }

  "comm毎月割一時金" should {
    "normal end with key" when {
      val result = df ~> comm毎月割一時金("key").run ~> ((_: DataFrame).cache)
      "N-02-05-01" in result ~> check("b", "0", "5")
      "N-02-05-02" in result ~> check("c", "0", "5")
      "N-02-05-03" in result ~> check("d", "5", "0")
      "N-02-05-04" in result ~> check("e", "1", "1")
    }

    "normal end without key" when {
      val result = df ~> comm毎月割一時金.run ~> ((_: DataFrame).cache)
      "N-02-05-01" in result ~> check("b", "0", "5")
      "N-02-05-02" in result ~> check("c", "0", "5")
      "N-02-05-03" in result ~> check("d", "5", "0")
      "N-02-05-04" in result ~> check("e", "1", "1")
    }
  }
}
