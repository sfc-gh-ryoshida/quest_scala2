package d2k.common.df

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.common.TestArgs
import d2k.common.InputArgs
import spark.common.SparkContexts
import SparkContexts.context.implicits._
import d2k.common.df.template.PqToDf
import d2k.common.df.executor.Nothing
import d2k.common.df.template.MultiPqToMapDf
import spark.common.PqCtl

object ReadPqTest {
  case class MultiReadTest(s: String)
}

class ReadPqTest extends WordSpec with MustMatchers with BeforeAndAfter {
  import ReadPqTest._
  implicit val inArgs = TestArgs().toInputArgs
  "strict check mode" should {
    "trueで指定したファイルが存在しない場合Exceptionが発生する" in {
      val pqToDf = new PqToDf with Nothing {
        val componentId = "ReadPqTest"
        override val readPqStrictCheckMode = true
      }
      try {
        pqToDf.run(Unit)
        fail
      } catch {
        case t: Throwable => {
          t.getClass.getName mustBe "org.apache.spark.sql.AnalysisException"
          t.getMessage must startWith("Path does not exist: file:")
        }
      }
    }

    "falseで指定したファイルが存在しない場合Exceptionが発生しない" when {
      "readPqEmptySchema未指定" in {
        val pqToDf = new PqToDf with Nothing {
          val componentId = "ReadPqTest"
          override val readPqStrictCheckMode = false
        }
        pqToDf.run(Unit)
      }

      "readPqEmptySchema指定" in {
        val pqToDf = new PqToDf with Nothing {
          val componentId = "ReadPqTest"
          override val readPqStrictCheckMode = false
          override val readPqEmptySchema = Seq(("a", "string"), ("b", "decimal"), ("c", "date"))
        }
        val schema = pqToDf.run(Unit).schema.map(_.toString)
        schema(0) mustBe "StructField(a,StringType,true)"
        schema(1) mustBe "StructField(b,DecimalType(10,0),true)"
        schema(2) mustBe "StructField(c,DateType,true)"
      }

      "readPqEmptySchema指定1" in {
        val pqToDf = new PqToDf with Nothing {
          val componentId = "ReadPqTest"
          override val readPqStrictCheckMode = false
          override val readPqEmptySchema = Seq(("VC_ADDUPYM", "String"), ("VC_ADDUPDT", "String"), ("VC_ADDUPYMD", "String"), ("CD_SISYAPARTITAFTSISYACD", "String"), ("DV_MNGCSCDIV", "String"), ("DV_PDCTCTGRY1", "String"), ("DV_PDCTCTGRY2", "String"), ("DV_INFTELKIND", "String"), ("DV_DATAPRIDIV", "String"), ("DV_MNPTRNMTCOCD", "String"), ("DV_MNPTRNSKCOCD", "String"), ("NM_NEWSU", "BigDecimal"), ("NM_MDLCHGCTRCP", "BigDecimal"), ("NM_MDLCHGCTHOLD", "BigDecimal"), ("NM_MDLCHGDECRHOLD", "BigDecimal"), ("NM_CNCLSU", "BigDecimal"), ("NM_SUMTTLWRKSU", "BigDecimal"), ("NM_INCRCOUNT", "BigDecimal"), ("NM_AUSMAVALSUMTTLCONTSU", "BigDecimal"), ("NM_MNPNEWSU", "BigDecimal"), ("NM_MNPCNCLSU", "BigDecimal"), ("NM_TDYDTNEWSU", "BigDecimal"), ("NM_TDYDTMDLCHGCTRCP", "BigDecimal"), ("NM_TDYDTMDLCHGCTHOLD", "BigDecimal"), ("NM_TDYDTMDLCHGDECRHOLD", "BigDecimal"), ("NM_TDYDTCNCLSU", "BigDecimal"), ("NM_TDYDTINCRCOUNT", "BigDecimal"), ("NM_TDYDTAUSMAVALINCRCOUNT", "BigDecimal"), ("NM_TDYDTMNPNEWSU", "BigDecimal"), ("NM_TDYDTMNPCNCLSU", "BigDecimal"))
        }
        pqToDf.run(Unit).schema.foreach(println)
      }
    }
  }

  "Single Read" should {
    "指定したParquetをDataFrameに変換できる" in {
      import SparkContexts.context.implicits._

      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._
      Seq(MultiReadTest("a")).toDF.writeParquet("A.pq")
      val mapDf = new PqToDf with Nothing {
        val componentId = "ReadPqTest"
        override lazy val readPqName = "A.pq"
      }

      val result = mapDf.run(Unit)
      result.collect.foreach { row =>
        row.getAs[String]("s") mustBe "a"
      }
    }
  }

  "Multi Read" should {
    "複数指定したParquetをDataFrameに変換できる" in {
      import SparkContexts.context.implicits._

      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._
      Seq(MultiReadTest("b")).toDF.writeParquet("B.pq")
      Seq(MultiReadTest("c")).toDF.writeParquet("C.pq")
      val mapDf = new MultiPqToMapDf with Nothing {
        val componentId = "ReadPqTest"
        override val readPqNames = Seq("B.pq", "C.pq")
      }

      val result = mapDf.run(Unit)
      result("B.pq").collect.foreach { row =>
        row.getAs[String]("s") mustBe "b"
      }
      result("C.pq").collect.foreach { row =>
        row.getAs[String]("s") mustBe "c"
      }
    }
  }

  "Many Read" should {
    "be normal end" in {
      import SparkContexts.context.implicits._

      val pqCtl = new PqCtl(inArgs.baseOutputFilePath)
      import pqCtl.implicits._
      Seq(MultiReadTest("b")).toDF.writeParquet("B.pq")
      Seq(MultiReadTest("c")).toDF.writeParquet("C.pq")
      val df = new PqToDf with Nothing {
        val componentId = "ReadPqTest"
        override lazy val readPqName = "B.pq, C.pq"
      }

      val result = df.run(Unit).collect
      result(0).getAs[String]("s") mustBe "b"
      result(1).getAs[String]("s") mustBe "c"
    }
  }
}