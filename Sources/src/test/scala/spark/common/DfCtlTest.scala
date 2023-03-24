package spark.common

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.functions._
import SparkContexts.context.implicits._
import spark.common.DfCtl._
import implicits._
import scala.reflect.io.Path
import org.apache.spark.sql.Column
import scala.io.Source

case class DfCtlData(
  a: String = "a", b: String = "b", c: String = "c", d: String = "d", e: String = "e",
  a1: Int = 1, b2: Int = 2)
case class DfCtlCastData(
  NM_A: String = "100", NM_B: String = "200", STR_C: String = "300", STR_D: String = "400", E: String = "500")
class DfCtlTest extends WordSpec with MustMatchers with BeforeAndAfter {
  val df = Seq(DfCtlData()).toDF
  "implicit conv editors" when {
    "edit" should {
      "be success" in {
        val d = Seq(
          ("a", col("c")).e,
          ("b", $"c").e)
        val r = df ~> editColumns(d)
        r.collect.foreach { row =>
          row.getAs[String]("a") mustBe "c"
          row.getAs[String]("b") mustBe "c"
        }
      }
    }

    "edit seq" should {
      "be success" in {
        val d = Seq(
          ("a", col("c")),
          ("b", $"c")).e
        val r = df ~> editColumns(d)
        r.collect.foreach { row =>
          row.getAs[String]("a") mustBe "c"
          row.getAs[String]("b") mustBe "c"
        }
      }
    }

    "rename" should {
      "be success" in {
        val d = Seq(
          ("a" -> "x").r,
          ("b" -> "y").r)
        val r = df ~> editColumns(d)
        r.collect.foreach { row =>
          row.getAs[String]("x") mustBe "a"
          row.getAs[String]("y") mustBe "b"
          val names = row.schema.map(_.name)
          names.contains("a") mustBe false
          names.contains("b") mustBe false
        }
      }
    }

    "rename seq" should {
      "be success" in {
        val d = Seq(
          ("a" -> "x"),
          ("b" -> "y")).r
        val r = df ~> editColumns(d)
        r.collect.foreach { row =>
          row.getAs[String]("x") mustBe "a"
          row.getAs[String]("y") mustBe "b"
          val names = row.schema.map(_.name)
          names.contains("a") mustBe false
          names.contains("b") mustBe false
        }
      }
    }

    "delete" should {
      "be success" in {
        val d = Seq(
          "a".d,
          "b".d)
        val r = df ~> editColumns(d)
        r.collect.foreach { row =>
          val names = row.schema.map(_.name)
          names.contains("a") mustBe false
          names.contains("b") mustBe false
        }
      }
    }

    "delete seq" should {
      "be success" in {
        val d = Seq(
          "a",
          "b").d
        val r = df ~> editColumns(d)
        r.collect.foreach { row =>
          val names = row.schema.map(_.name)
          names.contains("a") mustBe false
          names.contains("b") mustBe false
        }
      }
    }

    "cast by name" should {
      val df = Seq(DfCtlCastData()).toDF
      "be success" when {
        "direct" in {
          val d = Seq(
            ("NM_A", "bigint").c,
            ("NM_B", "int").c)
          val r = df ~> editColumns(d)
          val scm = r.schema
          scm("NM_A").dataType.typeName mustBe "long"
          scm("NM_B").dataType.typeName mustBe "integer"
          scm("STR_C").dataType.typeName mustBe "string"
          scm("STR_D").dataType.typeName mustBe "string"
          scm("E").dataType.typeName mustBe "string"
        }

        "regex" in {
          val d = Seq(
            ("NM_.*", "bigint").cr)
          val r = df ~> editColumns(d)
          val scm = r.schema
          scm("NM_A").dataType.typeName mustBe "long"
          scm("NM_B").dataType.typeName mustBe "long"
          scm("STR_C").dataType.typeName mustBe "string"
          scm("STR_D").dataType.typeName mustBe "string"
          scm("E").dataType.typeName mustBe "string"
        }

        "mix" in {
          val d = Seq(
            ("NM_A", "bigint").c,
            ("STR_.*", "decimal").cr,
            ("NM_B", "int").c)

          val r = df ~> editColumns(d)
          val scm = r.schema
          scm("NM_A").dataType.typeName mustBe "long"
          scm("NM_B").dataType.typeName mustBe "integer"
          scm("STR_C").dataType.typeName mustBe "decimal(10,0)"
          scm("STR_D").dataType.typeName mustBe "decimal(10,0)"
          scm("E").dataType.typeName mustBe "string"
        }
      }
    }

    "cast seq by name" should {
      val df = Seq(DfCtlCastData()).toDF
      "be success" when {
        "direct" in {
          val d = Seq(
            ("NM_A", "bigint"),
            ("NM_B", "int")).c
          val r = df ~> editColumns(d)
          val scm = r.schema
          scm("NM_A").dataType.typeName mustBe "long"
          scm("NM_B").dataType.typeName mustBe "integer"
          scm("STR_C").dataType.typeName mustBe "string"
          scm("STR_D").dataType.typeName mustBe "string"
          scm("E").dataType.typeName mustBe "string"
        }

        "regex" in {
          val d = Seq(
            ("NM_.*", "bigint"),
            ("STR_.*", "int")).cr
          val r = df ~> editColumns(d)
          val scm = r.schema
          scm("NM_A").dataType.typeName mustBe "long"
          scm("NM_B").dataType.typeName mustBe "long"
          scm("STR_C").dataType.typeName mustBe "integer"
          scm("STR_D").dataType.typeName mustBe "integer"
          scm("E").dataType.typeName mustBe "string"
        }
      }
    }

    "applyAll by name" should {
      val df = Seq(DfCtlCastData()).toDF
      "be success" when {
        "direct" in {
          val d = Seq(
            ("NM_A", (_: Column).cast("bigint")).a,
            ("NM_B", (_: Column).cast("int")).a)
          val r = df ~> editColumns(d)
          val scm = r.schema
          scm("NM_A").dataType.typeName mustBe "long"
          scm("NM_B").dataType.typeName mustBe "integer"
          scm("STR_C").dataType.typeName mustBe "string"
          scm("STR_D").dataType.typeName mustBe "string"
          scm("E").dataType.typeName mustBe "string"
        }

        "regex" in {
          val d = Seq(
            ("NM_.*", (_: Column).cast("bigint")).ar)
          val r = df ~> editColumns(d)
          val scm = r.schema
          scm("NM_A").dataType.typeName mustBe "long"
          scm("NM_B").dataType.typeName mustBe "long"
          scm("STR_C").dataType.typeName mustBe "string"
          scm("STR_D").dataType.typeName mustBe "string"
          scm("E").dataType.typeName mustBe "string"
        }

        "mix" in {
          val d = Seq(
            ("NM_A", (_: Column).cast("bigint")).a,
            ("STR_.*", (_: Column).cast("decimal")).ar,
            ("NM_B", (_: Column).cast("int")).a)

          val r = df ~> editColumns(d)
          val scm = r.schema
          scm("NM_A").dataType.typeName mustBe "long"
          scm("NM_B").dataType.typeName mustBe "integer"
          scm("STR_C").dataType.typeName mustBe "decimal(10,0)"
          scm("STR_D").dataType.typeName mustBe "decimal(10,0)"
          scm("E").dataType.typeName mustBe "string"
        }
      }
    }

    "applyAll seq by name" should {
      val df = Seq(DfCtlCastData()).toDF
      "be success" when {
        "direct" in {
          val d = Seq(
            ("NM_A", (_: Column).cast("bigint")),
            ("NM_B", (_: Column).cast("int"))).a
          val r = df ~> editColumns(d)
          val scm = r.schema
          scm("NM_A").dataType.typeName mustBe "long"
          scm("NM_B").dataType.typeName mustBe "integer"
          scm("STR_C").dataType.typeName mustBe "string"
          scm("STR_D").dataType.typeName mustBe "string"
          scm("E").dataType.typeName mustBe "string"
        }

        "regex" in {
          val d = Seq(
            ("NM_.*", (_: Column).cast("bigint")),
            ("STR_.*", (_: Column).cast("int"))).ar
          val r = df ~> editColumns(d)
          val scm = r.schema
          scm("NM_A").dataType.typeName mustBe "long"
          scm("NM_B").dataType.typeName mustBe "long"
          scm("STR_C").dataType.typeName mustBe "integer"
          scm("STR_D").dataType.typeName mustBe "integer"
          scm("E").dataType.typeName mustBe "string"
        }
      }
    }
  }

  "editColumns" should {
    "success" in {
      val editData =
        Seq(
          ("a", col("c")).e,
          ("b", $"d").e,
          ("c" -> "y").r,
          ("d" -> "z", lit("000")).r,
          "e".d)
      val r = df ~> editColumns(editData)
      r.schema.length mustBe 6
      r.collect.foreach { row =>
        row.getAs[String]("a") mustBe "c"
        row.getAs[String]("b") mustBe "d"
        row.getAs[String]("y") mustBe "c"
        row.getAs[String]("z") mustBe "000"
        val names = row.schema.map(_.name)
        names.contains("c") mustBe false
        names.contains("d") mustBe false
        names.contains("e") mustBe false
      }
    }
  }

  "editColumnsAndSelect" should {
    "success" in {
      val d = Seq(
        ("a", col("c")).e,
        ("b", $"d").e,
        ("c" -> "y").r,
        ("d" -> "z", lit("000")).r,
        "e".d)
      val r = df ~> editColumnsAndSelect(d)
      r.schema.length mustBe 4
      r.collect.foreach { row =>
        row.getAs[String]("a") mustBe "c"
        row.getAs[String]("b") mustBe "d"
        row.getAs[String]("y") mustBe "c"
        row.getAs[String]("z") mustBe "000"
        val names = row.schema.map(_.name)
        names.contains("c") mustBe false
        names.contains("d") mustBe false
        names.contains("e") mustBe false
      }
    }
  }

  "selectMaxValue" should {
    val df = Seq(DfCtlData(a1 = 0), DfCtlData(a1 = 100)).toDF
    "success" in {
      val r = df ~> selectMaxValue(Seq("a"), Seq($"a1".desc))
      r.count mustBe 1
      r.collect.foreach { row =>
        row.getAs[Int]("a1") mustBe 100
      }
    }
  }

  "groupingAgg" should {
    val df = Seq(DfCtlData(a1 = 0), DfCtlData(a1 = 100)).toDF
    "success" in {
      val r = df ~> groupingAgg(Seq("a"), Seq(avg("a1") as "a1"))
      r.count mustBe 1
      r.collect.foreach { row =>
        row.getAs[Int]("a1") mustBe 50
      }
    }
  }

  "groupingSum" should {
    val df = Seq(DfCtlData(a1 = 1), DfCtlData(a1 = 100)).toDF
    "success" in {
      val r = df ~> groupingSum(Seq("a"), Seq("a1"))
      r.count mustBe 1
      r.collect.foreach { row =>
        row.getAs[Int]("a1") mustBe 101
      }
    }
  }

  "addColumnPrefix" should {
    val df = Seq(DfCtlData(a1 = 1), DfCtlData(a1 = 100)).toDF
    "success" in {
      val r = df ~> addColumnPrefix("XXX")
      r.collect.foreach { row =>
        val names = row.schema.map(_.name)
        names.contains("XXX_a") mustBe true
        names.contains("XXX_b") mustBe true
      }
    }
  }

  "dropColumnPrefix" should {
    val df = Seq(DfCtlData(a1 = 1), DfCtlData(a1 = 100)).toDF
    val addPrefixCol = df ~> editColumns(Seq(("XXX_a", $"a"), ("XXX_b", $"b")).e)
    "success" in {
      val r = addPrefixCol ~> dropColumnPrefix("XXX")
      r.collect.foreach { row =>
        val names = row.schema.map(_.name)
        names.contains("XXX_a") mustBe false
        names.contains("XXX_b") mustBe false
      }
    }
  }

  def checkData(path: String, target: String) = {
    val basePath = Path(path)
    val files = basePath.jfile.listFiles
    val recs = files.flatMap(name => Source.fromFile(name).getLines)
    recs.contains(target)
  }
  "partitionWriteToFileWithPartitionColumns" should {
    "be normal end" when {
      "single column" in {
        val outPath = "test/ptt/t1"
        import FileCtl._
        val df = ('a' to 'c').flatMap(x => (1 to 3).map(cnt => FileCtlTest1(x.toString, cnt, cnt))).toDF
        df.partitionWriteToFileWithPartitionColumns(outPath, Seq("x"))(_.mkString)
        checkData(s"${outPath}/x=a", "a11.000000000000000000") mustBe true
        checkData(s"${outPath}/x=a", "a22.000000000000000000") mustBe true
        checkData(s"${outPath}/x=a", "a33.000000000000000000") mustBe true
        checkData(s"${outPath}/x=b", "b11.000000000000000000") mustBe true
        checkData(s"${outPath}/x=b", "b22.000000000000000000") mustBe true
        checkData(s"${outPath}/x=b", "b33.000000000000000000") mustBe true
        checkData(s"${outPath}/x=c", "c11.000000000000000000") mustBe true
        checkData(s"${outPath}/x=c", "c22.000000000000000000") mustBe true
        checkData(s"${outPath}/x=c", "c33.000000000000000000") mustBe true
      }

      "multi column" in {
        val outPath = "test/ptt/t2"
        import FileCtl._
        val df = ('a' to 'c').flatMap(x => (1 to 3).map(cnt => FileCtlTest1(x.toString, cnt, cnt))).toDF
        df.partitionWriteToFileWithPartitionColumns(outPath, Seq("x", "y"))(_.mkString)
        checkData(s"${outPath}/x=a/y=1", "a11.000000000000000000") mustBe true
        checkData(s"${outPath}/x=a/y=2", "a22.000000000000000000") mustBe true
        checkData(s"${outPath}/x=a/y=3", "a33.000000000000000000") mustBe true
        checkData(s"${outPath}/x=b/y=1", "b11.000000000000000000") mustBe true
        checkData(s"${outPath}/x=b/y=2", "b22.000000000000000000") mustBe true
        checkData(s"${outPath}/x=b/y=3", "b33.000000000000000000") mustBe true
        checkData(s"${outPath}/x=c/y=1", "c11.000000000000000000") mustBe true
        checkData(s"${outPath}/x=c/y=2", "c22.000000000000000000") mustBe true
        checkData(s"${outPath}/x=c/y=3", "c33.000000000000000000") mustBe true
      }
    }

    def extentionCheck(path: String, extention: String) =
      Path(path).jfile.listFiles.map(_.toString.endsWith(extention)).forall(_ == true)
    "add Extention" when {
      "single column" in {
        val outPath = "test/ptt/t3"
        import FileCtl._
        val df = ('a' to 'c').flatMap(x => (1 to 3).map(cnt => FileCtlTest1(x.toString, cnt, cnt))).toDF
        df.partitionWriteToFileWithPartitionColumns(outPath, Seq("x"), partitionExtention = "ext")(_.mkString)
        extentionCheck(s"${outPath}/x=a", "ext") mustBe true
        checkData(s"${outPath}/x=a", "a11.000000000000000000") mustBe true
        checkData(s"${outPath}/x=a", "a22.000000000000000000") mustBe true
        checkData(s"${outPath}/x=a", "a33.000000000000000000") mustBe true
        extentionCheck(s"${outPath}/x=b", "ext") mustBe true
        checkData(s"${outPath}/x=b", "b11.000000000000000000") mustBe true
        checkData(s"${outPath}/x=b", "b22.000000000000000000") mustBe true
        checkData(s"${outPath}/x=b", "b33.000000000000000000") mustBe true
        extentionCheck(s"${outPath}/x=c", "ext") mustBe true
        checkData(s"${outPath}/x=c", "c11.000000000000000000") mustBe true
        checkData(s"${outPath}/x=c", "c22.000000000000000000") mustBe true
        checkData(s"${outPath}/x=c", "c33.000000000000000000") mustBe true
      }

      "multi column" in {
        val outPath = "test/ptt/t4"
        import FileCtl._
        val df = ('a' to 'c').flatMap(x => (1 to 3).map(cnt => FileCtlTest1(x.toString, cnt, cnt))).toDF
        df.partitionWriteToFileWithPartitionColumns(outPath, Seq("x", "y"), partitionExtention = "ext")(_.mkString)
        extentionCheck(s"${outPath}/x=a/y=1", "ext") mustBe true
        checkData(s"${outPath}/x=a/y=1", "a11.000000000000000000") mustBe true
        extentionCheck(s"${outPath}/x=a/y=2", "ext") mustBe true
        checkData(s"${outPath}/x=a/y=2", "a22.000000000000000000") mustBe true
        extentionCheck(s"${outPath}/x=a/y=3", "ext") mustBe true
        checkData(s"${outPath}/x=a/y=3", "a33.000000000000000000") mustBe true
        extentionCheck(s"${outPath}/x=b/y=1", "ext") mustBe true
        checkData(s"${outPath}/x=b/y=1", "b11.000000000000000000") mustBe true
        extentionCheck(s"${outPath}/x=b/y=2", "ext") mustBe true
        checkData(s"${outPath}/x=b/y=2", "b22.000000000000000000") mustBe true
        extentionCheck(s"${outPath}/x=b/y=3", "ext") mustBe true
        checkData(s"${outPath}/x=b/y=3", "b33.000000000000000000") mustBe true
        extentionCheck(s"${outPath}/x=c/y=1", "ext") mustBe true
        checkData(s"${outPath}/x=c/y=1", "c11.000000000000000000") mustBe true
        extentionCheck(s"${outPath}/x=c/y=2", "ext") mustBe true
        checkData(s"${outPath}/x=c/y=2", "c22.000000000000000000") mustBe true
        extentionCheck(s"${outPath}/x=c/y=3", "ext") mustBe true
        checkData(s"${outPath}/x=c/y=3", "c33.000000000000000000") mustBe true
      }
    }
  }
}