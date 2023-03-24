package d2k.common.df.template

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import spark.common.SparkContexts.context
import spark.common.PqCtl

import d2k.common.df.executor
import d2k.common.df.template
import d2k.common.TestArgs
import d2k.common.df.VariableJoin
import d2k.common.df.PqInfo
import d2k.common.df.FixedInfo
import d2k.common.df.CsvInfo
import d2k.common.df.TsvInfo
import d2k.common.df.VsvInfo
import d2k.common.df.SsvInfo

class DfJoinVariableToDfTest extends WordSpec with MustMatchers with BeforeAndAfter {
  case class Test(key: String, value: String)
  case class Test2(key1: String, key2: String, value: String)
  case class Test3(key: String, value: String, value1: String, value2: String)

  "parquet" should {
    "be success single join data" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joins = Seq(VariableJoin(PqInfo("testPq1"), col("org#key1") === col("testPq1#key")))
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

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joins = Seq(
          VariableJoin(PqInfo("testPq1"), col("org#key1") === col("testPq1#key")),
          VariableJoin(PqInfo("testPq2"), col("org#key2") === col("testPq2#key")))
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

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joins = Seq(
          VariableJoin(PqInfo("testPq1"), col("org#key1") === col("testPq1#key")),
          VariableJoin(PqInfo("testPq2"), col("org#key2") === col("testPq2#key"), dropCols = Set("testPq2#value1")))
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

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joins = Seq(
          VariableJoin(PqInfo("testPq1"), col("org#key1") === col("pre1#key"), "pre1"),
          VariableJoin(PqInfo("testPq2"), col("org#key2") === col("pre2#key"), prefixName = "pre2"))
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
      result(0).getAs[String]("pre1#key") mustBe "key1"
      result(0).getAs[String]("pre1#value") mustBe "bbb"
      result(0).getAs[String]("pre2#key") mustBe "key2"
      result(0).getAs[String]("pre2#value") mustBe "ccc"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("pre1#key") mustBe null
      result(1).getAs[String]("pre1#value") mustBe null
      result(1).getAs[String]("pre2#key") mustBe "key22"
      result(1).getAs[String]("pre2#value") mustBe "ddd"
    }

    "be success env input data" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joins = Seq(VariableJoin(PqInfo("testPq3", "TEST"), col("org#key1") === col("testPq3#key")))
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

  "file common funcs" should {
    "be success single join data" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joins = Seq(
          VariableJoin(FixedInfo(Set("fixed.dat"), itemConfId = "fixed_change_conf_id"), col("org#key1") === col("fixed_change_conf_id#item1")))
      }

      val df1 = context.createDataFrame(Seq(Test2("a1", "key2", "aaa"), Test2("key11", "key22", "xxx")))

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "a1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("fixed_change_conf_id#item1") mustBe "a1"
      result(0).getAs[String]("fixed_change_conf_id#item2") mustBe "bb1"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("fixed_change_conf_id#item1") mustBe null
      result(1).getAs[String]("fixed_change_conf_id#item2") mustBe null
    }

    "be success any joined data" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joins = Seq(
          VariableJoin(FixedInfo(Set("fixed.dat"), itemConfId = "fixed_change_conf_id"), col("org#key1") === col("fixed_change_conf_id#item1")),
          VariableJoin(FixedInfo(Set("fixed.dat"), itemConfId = "fixed_change_conf_id"), col("org#key1") === col("pre#item1"), "pre"))
      }

      val df1 = context.createDataFrame(Seq(Test2("a1", "key2", "aaa"), Test2("key11", "key22", "xxx")))

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "a1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("fixed_change_conf_id#item1") mustBe "a1"
      result(0).getAs[String]("fixed_change_conf_id#item2") mustBe "bb1"
      result(0).getAs[String]("pre#item1") mustBe "a1"
      result(0).getAs[String]("pre#item2") mustBe "bb1"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("fixed_change_conf_id#item1") mustBe null
      result(1).getAs[String]("fixed_change_conf_id#item2") mustBe null
      result(1).getAs[String]("pre#item1") mustBe null
      result(1).getAs[String]("pre#item2") mustBe null
    }

    "be success file not found" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joins = Seq(
          VariableJoin(FixedInfo(Set("notFound.dat"), itemConfId = "fixed_change_conf_id"), col("org#key1") === col("fixed_change_conf_id#item1")))
      }

      val df1 = context.createDataFrame(Seq(Test2("a1", "key2", "aaa"), Test2("key11", "key22", "xxx")))

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "a1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("fixed_change_conf_id#item1") mustBe null
      result(0).getAs[String]("fixed_change_conf_id#item2") mustBe null

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("fixed_change_conf_id#item1") mustBe null
      result(1).getAs[String]("fixed_change_conf_id#item2") mustBe null
    }

    "be success one found and one not found" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joins = Seq(
          VariableJoin(FixedInfo(Set("notFound.dat"), itemConfId = "fixed_change_conf_id"), col("org#key1") === col("fixed_change_conf_id#item1")),
          VariableJoin(FixedInfo(Set("fixed.dat"), itemConfId = "fixed_change_conf_id"), col("org#key1") === col("pre#item1"), "pre"))
      }

      val df1 = context.createDataFrame(Seq(Test2("a1", "key2", "aaa"), Test2("key11", "key22", "xxx")))

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "a1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("fixed_change_conf_id#item1") mustBe null
      result(0).getAs[String]("fixed_change_conf_id#item2") mustBe null
      result(0).getAs[String]("pre#item1") mustBe "a1"
      result(0).getAs[String]("pre#item2") mustBe "bb1"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("fixed_change_conf_id#item1") mustBe null
      result(1).getAs[String]("fixed_change_conf_id#item2") mustBe null
      result(1).getAs[String]("pre#item1") mustBe null
      result(1).getAs[String]("pre#item2") mustBe null
    }

    "be success non droped row error column" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"
        val joins = Seq(
          VariableJoin(FixedInfo(Set("fixed.dat"), itemConfId = "fixed_change_conf_id", dropRowError = false), col("org#key1") === col("fixed_change_conf_id#item1")))
      }

      val df1 = context.createDataFrame(Seq(Test2("a1", "key2", "aaa"), Test2("key11", "key22", "xxx")))

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "a1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("fixed_change_conf_id#item1") mustBe "a1"
      result(0).getAs[String]("fixed_change_conf_id#item2") mustBe "bb1"
      result(0).getAs[String]("fixed_change_conf_id#ROW_ERR") mustBe "false"
      result(0).getAs[String]("fixed_change_conf_id#ROW_ERR_MESSAGE") mustBe ""

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("fixed_change_conf_id#item1") mustBe null
      result(1).getAs[String]("fixed_change_conf_id#item2") mustBe null
      result(1).getAs[String]("fixed_change_conf_id#ROW_ERR") mustBe null
      result(1).getAs[String]("fixed_change_conf_id#ROW_ERR_MESSAGE") mustBe null
    }
  }

  "csv" should {
    "be success single join data" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"

        val joins = Seq(
          VariableJoin(CsvInfo(Set("csv.dat"), itemConfId = "csv"), col("org#key1") === col("pre#item1"), "pre"))
      }

      val df1 = context.createDataFrame(Seq(Test2("a1", "key2", "aaa"), Test2("key11", "key22", "xxx")))

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "a1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("pre#item1") mustBe "a1"
      result(0).getAs[String]("pre#item2") mustBe "bb1"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("pre#item1") mustBe null
      result(1).getAs[String]("pre#item2") mustBe null
    }
  }

  "tsv" should {
    "be success single join data" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"

        val joins = Seq(
          VariableJoin(TsvInfo(Set("tsv.dat"), itemConfId = "tsv"), col("org#key1") === col("pre#item1"), "pre"))
      }

      val df1 = context.createDataFrame(Seq(Test2("a1", "key2", "aaa"), Test2("key11", "key22", "xxx")))

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "a1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("pre#item1") mustBe "a1"
      result(0).getAs[String]("pre#item2") mustBe "bb1"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("pre#item1") mustBe null
      result(1).getAs[String]("pre#item2") mustBe null
    }
  }

  "vsv" should {
    "be success single join data" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"

        val joins = Seq(
          VariableJoin(VsvInfo(Set("vsv.dat"), itemConfId = "vsv"), col("org#key1") === col("pre#item1"), "pre"))
      }

      val df1 = context.createDataFrame(Seq(Test2("a1", "key2", "aaa"), Test2("key11", "key22", "xxx")))

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "a1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("pre#item1") mustBe "a1"
      result(0).getAs[String]("pre#item2") mustBe "bb1"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("pre#item1") mustBe null
      result(1).getAs[String]("pre#item2") mustBe null
    }
  }

  "ssv" should {
    "be success single join data" in {
      implicit val inArgs = TestArgs().toInputArgs
      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._

      val comp = new template.DfJoinVariableToDf with executor.Nothing {
        val componentId = "test"
        val prefixName = "org"

        val joins = Seq(
          VariableJoin(SsvInfo(Set("ssv.dat"), itemConfId = "csv"), col("org#key1") === col("pre#item1"), "pre"))
      }

      val df1 = context.createDataFrame(Seq(Test2("a1", "key2", "aaa"), Test2("key11", "key22", "xxx")))

      val result = comp.run(df1).sort("org#key1").collect
      result(0).getAs[String]("org#key1") mustBe "a1"
      result(0).getAs[String]("org#key2") mustBe "key2"
      result(0).getAs[String]("org#value") mustBe "aaa"
      result(0).getAs[String]("pre#item1") mustBe "a1"
      result(0).getAs[String]("pre#item2") mustBe "bb1"

      result(1).getAs[String]("org#key1") mustBe "key11"
      result(1).getAs[String]("org#key2") mustBe "key22"
      result(1).getAs[String]("org#value") mustBe "xxx"
      result(1).getAs[String]("pre#item1") mustBe null
      result(1).getAs[String]("pre#item2") mustBe null
    }
  }
}
