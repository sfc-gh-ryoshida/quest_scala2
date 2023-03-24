package d2k.common.df.template

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts.context
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import d2k.common.df.executor
import d2k.common.df.template
import d2k.common.df.template.base.JoinPqInfo
import d2k.common.TestArgs
import spark.common.PqCtl

class DfJoinPqToDfTest extends WordSpec with MustMatchers with BeforeAndAfter {
  case class Test(key: String, value: String)
  case class Test2(key1: String, key2: String, value: String)
  case class Test3(key: String, value: String, value1: String, value2: String)

  "DfJoinPqToDf" should {
    "be success single data" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinPqToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joinPqInfoList = Seq(
          JoinPqInfo("testPq1", col("org#key1") === col("testPq1#key")))
      }

      val df1 = context.createDataFrame(Seq(Test2("key1", "key2", "aaa"), Test2("key11", "key22", "xxx")))
      val df2 = context.createDataFrame(Seq(Test("key1", "bbb")))
      df2.writeParquet("testPq1")

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "key1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("testPq1#key") mustBe "key1"
      result(0).getAs[String]("testPq1#value") mustBe "bbb"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("testPq1#key") mustBe null
      result(1).getAs[String]("testPq1#value") mustBe null
    }

    "be success multi data" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinPqToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joinPqInfoList = Seq(
          JoinPqInfo("testPq1", col("org#key1") === col("testPq1#key")),
          JoinPqInfo("testPq2", col("org#key2") === col("testPq2#key")))
      }

      val df1 = context.createDataFrame(Seq(Test2("key1", "key2", "aaa"), Test2("key11", "key22", "xxx")))
      val df2 = context.createDataFrame(Seq(Test("key1", "bbb")))
      df2.writeParquet("testPq1")
      val df3 = context.createDataFrame(Seq(Test("key2", "ccc"), Test("key22", "ddd")))
      df3.writeParquet("testPq2")

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "key1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("testPq1#key") mustBe "key1"
      result(0).getAs[String]("testPq1#value") mustBe "bbb"
      result(0).getAs[String]("testPq2#key") mustBe "key2"
      result(0).getAs[String]("testPq2#value") mustBe "ccc"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("testPq1#key") mustBe null
      result(1).getAs[String]("testPq1#value") mustBe null
      result(1).getAs[String]("testPq2#key") mustBe "key22"
      result(1).getAs[String]("testPq2#value") mustBe "ddd"
    }

    "be success drop column" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinPqToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joinPqInfoList = Seq(
          JoinPqInfo("testPq1", col("org#key1") === col("testPq1#key")),
          JoinPqInfo("testPq2", col("org#key2") === col("testPq2#key"), Set("testPq2#value1")))
      }

      val df1 = context.createDataFrame(Seq(Test2("key1", "key2", "aaa"), Test2("key11", "key22", "xxx")))
      val df2 = context.createDataFrame(Seq(Test("key1", "bbb")))
      df2.writeParquet("testPq1")
      val df3 = context.createDataFrame(Seq(
        Test3("key2", "ccc", "ccc1", "ccc2"),
        Test3("key22", "ddd", "ddd1", "ddd2")))
      df3.writeParquet("testPq2")

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "key1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("testPq1#key") mustBe "key1"
      result(0).getAs[String]("testPq1#value") mustBe "bbb"
      result(0).getAs[String]("testPq2#key") mustBe "key2"
      result(0).getAs[String]("testPq2#value") mustBe "ccc"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("testPq1#key") mustBe null
      result(1).getAs[String]("testPq1#value") mustBe null
      result(1).getAs[String]("testPq2#key") mustBe "key22"
      result(1).getAs[String]("testPq2#value") mustBe "ddd"
    }

    "be success change prefix name" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinPqToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joinPqInfoList = Seq(
          JoinPqInfo("testPq1", col("org#key1") === col("pre#key"), prefixName = "pre"),
          JoinPqInfo("testPq2", col("org#key2") === col("testPq2#key")))
      }

      val df1 = context.createDataFrame(Seq(Test2("key1", "key2", "aaa"), Test2("key11", "key22", "xxx")))
      val df2 = context.createDataFrame(Seq(Test("key1", "bbb")))
      df2.writeParquet("testPq1")
      val df3 = context.createDataFrame(Seq(Test("key2", "ccc"), Test("key22", "ddd")))
      df3.writeParquet("testPq2")

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "key1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("pre#key") mustBe "key1"
      result(0).getAs[String]("pre#value") mustBe "bbb"
      result(0).getAs[String]("testPq2#key") mustBe "key2"
      result(0).getAs[String]("testPq2#value") mustBe "ccc"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("pre#key") mustBe null
      result(1).getAs[String]("pre#value") mustBe null
      result(1).getAs[String]("testPq2#key") mustBe "key22"
      result(1).getAs[String]("testPq2#value") mustBe "ddd"
    }

    "be success env input data" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinPqToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joinPqInfoList = Seq(
          JoinPqInfo("testPq3", col("org#key1") === col("testPq3#key")))
        override lazy val envName = "TEST"
      }

      val df1 = context.createDataFrame(Seq(Test2("key1", "key2", "aaa"), Test2("key11", "key22", "xxx")))
      val df2 = context.createDataFrame(Seq(Test("key1", "bbb")))
      df2.writeParquet("testPq3")

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "key1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("testPq3#key") mustBe "key1"
      result(0).getAs[String]("testPq3#value") mustBe "bbb"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("testPq3#key") mustBe null
      result(1).getAs[String]("testPq3#value") mustBe null
    }

  }
}
