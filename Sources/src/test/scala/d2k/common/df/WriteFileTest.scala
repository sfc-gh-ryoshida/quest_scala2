package d2k.common.df

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.common.TestArgs
import d2k.common.InputArgs
import spark.common.SparkContexts
import SparkContexts.context.implicits._
import scala.io.Source
import d2k.common.df.WriteFileMode._
import org.apache.spark.sql.types._
import spark.common.SparkContexts.context
import context.implicits._
import org.apache.spark.sql.Row
import spark.common.DbCtl

case class TestData(a: String, b: String)
class WriteFileTest extends WordSpec with MustMatchers with BeforeAndAfter {
  implicit val inArgs = TestArgs().toInputArgs
  val df = SparkContexts.sc.makeRDD(Seq(TestData("aaa", "bbb"), TestData("ccccc", "ddddd"), TestData(null, null))).toDF

  "WriteFileTest" should {
    "be normal end. Fixed with arg pattern" in {
      val writeFile = new WriteFile {
        val componentId = "writeFile1"
        override val writeFileMode = Fixed(2, 5)
      }
      writeFile.writeFile(df)
      val lines = Source.fromFile(s"${inArgs.baseOutputFilePath}/writeFile1").getLines.toList
      lines(0) mustBe "aabbb  "
      lines(1) mustBe "ccddddd"
      lines(2) mustBe "       "
    }

    "be normal end. Fixed with empty arg pattern" in {
      val writeFile = new WriteFile {
        val componentId = "writeFile11"
        override val writeFileMode = Fixed()
      }
      writeFile.writeFile(df)
      val lines = Source.fromFile(s"${inArgs.baseOutputFilePath}/writeFile11").getLines.toList
      lines(0) mustBe ""
      lines(1) mustBe ""
      lines(2) mustBe ""
    }

    "be normal end. Csv with arg pattern. writeFileVariableWrapDoubleQuote = true(default)" in {
      val writeFile = new WriteFile {
        val componentId = "writeFile2"
        override val writeFileMode = Csv("a")
      }
      writeFile.writeFile(df)
      val lines = Source.fromFile(s"${inArgs.baseOutputFilePath}/writeFile2").getLines.toList
      lines(0) mustBe """"aaa",bbb"""
      lines(1) mustBe """"ccccc",ddddd"""
      lines(2) mustBe """"","""
    }

    "be normal end. Csv with arg pattern. writeFileVariableWrapDoubleQuote = false" in {
      val writeFile = new WriteFile {
        val componentId = "writeFile2-2"
        override val writeFileMode = Csv("a")
        override val writeFileVariableWrapDoubleQuote = false
      }
      writeFile.writeFile(df)
      val lines = Source.fromFile(s"${inArgs.baseOutputFilePath}/writeFile2-2").getLines.toList
      lines(0) mustBe """"aaa",bbb"""
      lines(1) mustBe """"ccccc",ddddd"""
      lines(2) mustBe ""","""
    }

    "be normal end. Csv with empty arg pattern" in {
      val writeFile = new WriteFile {
        val componentId = "writeFile2x"
        override val writeFileMode = Csv()
      }
      writeFile.writeFile(df)
      val lines = Source.fromFile(s"${inArgs.baseOutputFilePath}/writeFile2x").getLines.toList
      lines(0) mustBe """aaa,bbb"""
      lines(1) mustBe """ccccc,ddddd"""
      lines(2) mustBe ""","""
    }
  }

  "CommonServices.writeFile CSV" should {
    val structType = StructType(Seq(
      StructField("KEY", StringType), StructField("TEST", StringType)))

    "normal end" in {
      implicit val inArgs = TestArgs().toInputArgs
      val forInsert = new WriteFile {
        val componentId = "csv"
        override val writeFileMode = Csv
      }
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", "あああ～"), Row("key2", """bb"b"""), Row("key3", """c"c"c"""))), structType)
      forInsert.writeFile(df)

      val result = Source.fromFile(s"${inArgs.baseOutputFilePath}/csv")("MS932").getLines.toList.sorted
      result(0) mustBe """"key1","あああ～""""
      result(1) mustBe """"key2","bb""b""""
      result(2) mustBe """"key3","c""c""c""""
    }

    "changeFileName" in {
      implicit val inArgs = TestArgs().toInputArgs
      val forInsert = new WriteFile {
        val componentId = "csv"
        override val writeFileMode = Csv
        override lazy val writeFileName = "xxx"
      }
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", "あああ～"))), structType)
      forInsert.writeFile(df)

      val result = Source.fromFile(s"${inArgs.baseOutputFilePath}/xxx")("MS932").getLines.toList.sorted
      result(0) mustBe """"key1","あああ～""""
    }

    "change Non Double Quote" in {
      implicit val inArgs = TestArgs().toInputArgs
      val forInsert = new WriteFile {
        val componentId = "csv2"
        override val writeFileMode = Csv
        override val writeFileVariableWrapDoubleQuote = false
      }
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", "あああ～"))), structType)
      forInsert.writeFile(df)

      val result = Source.fromFile(s"${inArgs.baseOutputFilePath}/csv2")("MS932").getLines.toList.sorted
      result(0) mustBe """key1,あああ～"""
    }

    "empty DF" in {
      implicit val inArgs = TestArgs().toInputArgs
      val common = new WriteFile {
        val componentId = "csv"
        override val writeFileMode = Csv
        override lazy val writeFileName = "test2"
      }
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq.empty[Row]), structType)
      common.writeFile(df)

      try {
        Source.fromFile(s"${inArgs.baseOutputFilePath}/csv2")("MS932")
        fail
      } catch {
        case t: Throwable => t.printStackTrace()
      }
    }

    "null column" in {
      implicit val inArgs = TestArgs().toInputArgs
      val common = new WriteFile {
        val componentId = "csv"
        override val writeFileMode = Csv
        override lazy val writeFileName = "test3"
      }
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", null))), structType)
      common.writeFile(df)

      val result = Source.fromFile(s"${inArgs.baseOutputFilePath}/test3")("MS932").getLines.toList.sorted
      result(0) mustBe """"key1","""""
    }
  }

  "CommonServices.writeFile TSV" should {
    val structType = StructType(Seq(
      StructField("KEY", StringType), StructField("TEST", StringType)))

    "normal end" in {
      implicit val inArgs = TestArgs().toInputArgs
      val forInsert = new WriteFile {
        val componentId = "tsv"
        override val writeFileMode = Tsv
      }
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", "あああ～"), Row("key2", """bb"b"""), Row("key3", """c"c"c"""))), structType)
      forInsert.writeFile(df)

      val result = Source.fromFile(s"${inArgs.baseOutputFilePath}/tsv")("MS932").getLines.toList.sorted
      result(0) mustBe "\"key1\"\t\"あああ～\""
      result(1) mustBe "\"key2\"\t\"bb\"\"b\""
      result(2) mustBe "\"key3\"\t\"c\"\"c\"\"c\""
    }

    "changeFileName" in {
      implicit val inArgs = TestArgs().toInputArgs
      val forInsert = new WriteFile {
        val componentId = "tsv"
        override val writeFileMode = Tsv
        override lazy val writeFileName = "xxx2"
      }
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", "あああ～"))), structType)
      forInsert.writeFile(df)

      val result = Source.fromFile(s"${inArgs.baseOutputFilePath}/xxx2")("MS932").getLines.toList.sorted
      result(0) mustBe "\"key1\"\t\"あああ～\""
    }

    "change Non Double Quote" in {
      implicit val inArgs = TestArgs().toInputArgs
      val forInsert = new WriteFile {
        val componentId = "tsv2"
        override val writeFileMode = Tsv
        override val writeFileVariableWrapDoubleQuote = false
      }
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", "あああ～"))), structType)
      forInsert.writeFile(df)

      val result = Source.fromFile(s"${inArgs.baseOutputFilePath}/tsv2")("MS932").getLines.toList.sorted
      result(0) mustBe "key1\tあああ～"
    }

    "empty DF" in {
      implicit val inArgs = TestArgs().toInputArgs
      val common = new WriteFile {
        val componentId = "tsv"
        override val writeFileMode = Tsv
        override lazy val writeFileName = "test2"
      }
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq.empty[Row]), structType)
      common.writeFile(df)

      try {
        Source.fromFile(s"${inArgs.baseOutputFilePath}/tsv2")("MS932")
        fail
      } catch {
        case t: Throwable => t.printStackTrace()
      }
    }
  }

  "CommonServices.DbInfo" should {
    implicit val inArgs = TestArgs().toInputArgs
    "not defined(Default settting)" in {
      val target = new WriteDb with ReadDb {
        val componentId = "dbInfoTest"
      }
      target.readDbInfo mustBe DbCtl.dbInfo1
      target.writeDbInfo mustBe DbCtl.dbInfo1
    }

    "readDbInf" in {
      val target = new WriteDb with ReadDb {
        val componentId = "dbInfoTest"
        override val readDbInfo = DbCtl.dbInfo2
      }
      target.readDbInfo mustBe DbCtl.dbInfo2
      target.writeDbInfo mustBe DbCtl.dbInfo1
    }

    "writeDbInf" in {
      val target = new WriteDb with ReadDb {
        val componentId = "dbInfoTest"
        override val writeDbInfo = DbCtl.dbInfo2
      }
      target.readDbInfo mustBe DbCtl.dbInfo1
      target.writeDbInfo mustBe DbCtl.dbInfo2
    }

    "both" in {
      val target = new WriteDb with ReadDb {
        val componentId = "dbInfoTest"
        override val readDbInfo = DbCtl.dbInfo2
        override val writeDbInfo = DbCtl.dbInfo2
      }
      target.readDbInfo mustBe DbCtl.dbInfo2
      target.writeDbInfo mustBe DbCtl.dbInfo2
    }
  }

  "Override ResourcePath" should {
    val structType = StructType(Seq(
      StructField("KEY", StringType), StructField("TEST", StringType)))
    "normal end" in {
      implicit val inArgs = TestArgs().toInputArgs
      val forInsert = new WritePq with SingleReadPq with WriteFile {
        val componentId = "csv"
        override val writeFileMode = Csv
        override def readPqPath(implicit inArgs: InputArgs): String = s"test/dev/data/mypath"
        override def writePqPath(implicit inArgs: InputArgs): String = s"test/dev/data/mypath"
        override def writeFilePath(implicit inArgs: InputArgs): String = s"test/dev/data/mypath2"
      }
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", "あああ～"), Row("key2", """bb"b"""), Row("key3", """c"c"c"""))), structType)
      forInsert.writeFile(df)
      forInsert.writeParquet(df)
      forInsert.readParquet

      val result = Source.fromFile(s"test/dev/data/mypath2/csv")("MS932").getLines.toList.sorted
      result(0) mustBe """"key1","あああ～""""
      result(1) mustBe """"key2","bb""b""""
      result(2) mustBe """"key3","c""c""c""""
    }

    "shared Impliments" in {
      implicit val inArgs = TestArgs().toInputArgs
      val forInsert = new WritePq with SingleReadPq with WriteFile {
        val componentId = "csv2"
        override def readPqPath(implicit inArgs: InputArgs): String = s"test/dev/data/mypath"
        override def writePqPath(implicit inArgs: InputArgs): String = s"test/dev/data/mypath"
        override def writeFilePath(implicit inArgs: InputArgs): String = s"test/dev/data/mypath"
        override val writeFileMode = Csv
        override lazy val readPqName = "pq"
        override lazy val writePqName = "pq"
      }
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key11", "あああ～"), Row("key12", """bb"b"""), Row("key13", """c"c"c"""))), structType)
      forInsert.writeFile(df)
      forInsert.writeParquet(df)
      forInsert.readParquet
    }
  }

  "writeCharEncoding by UTF-8" should {
    val testData = Seq(TestData("aaa", "bbb"), TestData("あ～", "い～"), TestData("", "")).toDF
    "csv" in {
      val writeFile = new WriteFile {
        val componentId = "utf8TestForCsv"
        override val writeFileMode = Csv
        override val writeCharEncoding = "UTF-8"
      }
      val df = writeFile.writeFile(testData)
      val lines = Source.fromFile(s"${inArgs.baseOutputFilePath}/${writeFile.componentId}")("UTF-8").getLines.toList
      lines(0) mustBe "\"aaa\",\"bbb\""
      lines(1) mustBe "\"あ～\",\"い～\""
      lines(2) mustBe "\"\",\"\""
    }

    "tsv" in {
      val writeFile = new WriteFile {
        val componentId = "utf8TestForTsv"
        override val writeFileMode = Tsv
        override val writeCharEncoding = "UTF-8"
      }
      val df = writeFile.writeFile(testData)
      val lines = Source.fromFile(s"${inArgs.baseOutputFilePath}/${writeFile.componentId}")("UTF-8").getLines.toList
      lines(0) mustBe "\"aaa\"\t\"bbb\""
      lines(1) mustBe "\"あ～\"\t\"い～\""
      lines(2) mustBe "\"\"\t\"\""
    }
  }
}
