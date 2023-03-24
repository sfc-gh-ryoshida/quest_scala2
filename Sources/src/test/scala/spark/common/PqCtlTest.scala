package spark.common

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import scala.reflect.io.Directory
import SparkContexts.context.implicits._
import scala.util.Try

case class PqCtlTestData(a: String)
class PqCtlTest extends WordSpec with MustMatchers with BeforeAndAfter {
  val path = "test/pqtest"
  val name = "test"
  val pqCtl = new PqCtl(path)
  import pqCtl.implicits._
  Seq(PqCtlTestData("aaa")).toDF.writeParquet(name)

  "Strict Mode check" should {
    "be success. mode true" in {
      try {
        pqCtl.readParquet("testx", true)
        fail
      } catch {
        case t: Throwable => {
          t.getClass.getName mustBe "org.apache.spark.sql.AnalysisException"
          t.getMessage must startWith("Path does not exist:")
        }
      }
    }

    "be success. mode true. pattern2" in {
      try {
        pqCtl.readParquet("testx/*/*/aaa", true)
        fail
      } catch {
        case t: Throwable => {
          t.getClass.getName mustBe "org.apache.spark.sql.AnalysisException"
          t.getMessage must startWith("Path does not exist:")
        }
      }
    }

    "be success. mode false" in {
      pqCtl.readParquet("testx", false)
    }
  }

  "many read paths" should {
    "be normal end" when {
      Seq(PqCtlTestData("aaa")).toDF.writeParquet("manyPath1.pq")
      Seq(PqCtlTestData("bbb")).toDF.writeParquet("manyPath2.pq")
      "normal mode" in {
        val df = pqCtl.readParquet("manyPath1.pq , manyPath2.pq", false)
        val result = df.collect
        result(0).getAs[String]("a") mustBe "aaa"
        result(1).getAs[String]("a") mustBe "bbb"
      }

      "strict mode" in {
        val df = pqCtl.readParquet("manyPath1.pq , manyPath2.pq", true)
        val result = df.collect
        result(0).getAs[String]("a") mustBe "aaa"
        result(1).getAs[String]("a") mustBe "bbb"
      }
    }
  }
}