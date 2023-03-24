package spark.common

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import scala.reflect.io.Directory
import spark.common.SparkContexts.context
import context.implicits._
import java.io.PrintWriter
import scala.util.Try
import scala.reflect.io.Path

case class FileCtlTest1(x: String, y: Int, z: BigDecimal)
class FileCtlTest extends WordSpec with MustMatchers with BeforeAndAfter {
  "FileCtl" should {
    "exists" in {
      FileCtl.exists("build.sbt") mustBe true
    }

    "not exists" in {
      FileCtl.exists("build.sxx") mustBe false
    }

    "exists wildCard" in {
      FileCtl.exists("*.sbt") mustBe true
      FileCtl.exists("[a-b]uild.sbt") mustBe true
    }

    "not exists wildCard" in {
      FileCtl.exists("*.sxx") mustBe false
      FileCtl.exists("[c-d]uild.sbt") mustBe false
    }

    "success deleteDirectory1" in {
      val d = (Directory("test") / "dummyDir")
      d.createDirectory(true, false)
      val f = d / "dummyFile"
      f.createFile(false)

      FileCtl.deleteDirectory(d.toFile.toString)
      d.exists mustBe false
    }

    "success deleteDirectory2" in {
      val d = (Directory("test") / "dummyDir")
      d.createDirectory(true, false)
      val f = d / "dummyFile"
      f.createFile(false)

      FileCtl.deleteDirectory(Directory("test").toString, "dummyDir")
      d.exists mustBe false
    }

    "with partition columns" when {
      "one Column" in {
        val outPath = "test/wpc1"
        FileCtl.deleteDirectory(outPath)

        val df = ('a' to 'c').flatMap(x => (1 to 3).map(cnt => FileCtlTest1(x.toString, cnt, cnt))).toDF
        FileCtl.loanPrintWriterCache { cache =>
          df.collect.foldLeft(cache) { (l, r) =>
            FileCtl.writeToFileWithPartitionColumns(outPath, partitionColumns = Seq("x"))(_.mkString(","))(l)(r)
          }
        }
        ('a' to 'c').flatMap(x => (1 to 3).map(cnt => s"${outPath}/x=${x}/0")).foreach { path =>
          withClue(path) { Path(path).exists mustBe true }
        }
      }

      "two Columns" in {
        val outPath = "test/wpc2"
        FileCtl.deleteDirectory(outPath)

        val df = ('a' to 'c').flatMap(x => (1 to 3).map(cnt => FileCtlTest1(x.toString, cnt, cnt))).toDF
        FileCtl.loanPrintWriterCache { cache =>
          df.collect.foldLeft(cache) { (l, r) =>
            FileCtl.writeToFileWithPartitionColumns(outPath, partitionColumns = Seq("x", "y"))(_.mkString(","))(l)(r)
          }
        }
        ('a' to 'c').flatMap(x => (1 to 3).map(cnt => s"${outPath}/x=${x}/y=${cnt}/0")).foreach { path =>
          withClue(path) { Path(path).exists mustBe true }
        }
      }
    }

    "with partition columns add Extention" when {
      "one Column" in {
        val outPath = "test/wpc1"
        FileCtl.deleteDirectory(outPath)

        val df = ('a' to 'c').flatMap(x => (1 to 3).map(cnt => FileCtlTest1(x.toString, cnt, cnt))).toDF
        FileCtl.loanPrintWriterCache { cache =>
          df.collect.foldLeft(cache) { (l, r) =>
            FileCtl.writeToFileWithPartitionColumns(outPath, partitionColumns = Seq("x"), partitionExtention = "ext")(_.mkString(","))(l)(r)
          }
        }
        ('a' to 'c').flatMap(x => (1 to 3).map(cnt => s"${outPath}/x=${x}/0.ext")).foreach { path =>
          withClue(path) { Path(path).exists mustBe true }
        }
      }

      "two Columns" in {
        val outPath = "test/wpc2"
        FileCtl.deleteDirectory(outPath)

        val df = ('a' to 'c').flatMap(x => (1 to 3).map(cnt => FileCtlTest1(x.toString, cnt, cnt))).toDF
        FileCtl.loanPrintWriterCache { cache =>
          df.collect.foldLeft(cache) { (l, r) =>
            FileCtl.writeToFileWithPartitionColumns(outPath, partitionColumns = Seq("x", "y"), partitionExtention = "ext")(_.mkString(","))(l)(r)
          }
        }
        ('a' to 'c').flatMap(x => (1 to 3).map(cnt => s"${outPath}/x=${x}/y=${cnt}/0.ext")).foreach { path =>
          withClue(path) { Path(path).exists mustBe true }
        }
      }
    }
  }
}