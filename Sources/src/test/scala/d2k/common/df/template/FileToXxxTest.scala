package d2k.common.df.template

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts.context
import context.implicits._
import d2k.common.InputArgs
import d2k.common.TestArgs
import d2k.common.df._
import d2k.common.fileConv.FixedConverter
import d2k.common.fileConv.Converter
import d2k.common.df.FileInputInfoBase.CRLF
import d2k.common.df.FileInputInfoBase.CR

class FileToXxxTest extends WordSpec with MustMatchers with BeforeAndAfter {
  "NoConfFileToXxx for csv" should {
    implicit val inArgs = TestArgs().toInputArgs

    "success simplePattern test" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "csv"
        val fileInputInfo = CsvInfo(Set("csv.dat"))
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success cnange conf id" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "csv"
        override lazy val itemConfId = "csv_change_conf_id"
        val fileInputInfo = CsvInfo(Set("csv.dat"))
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success file path from componentId and change conf id" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fptest"
        val fileInputInfo = CsvInfo(Set("csv.dat"))
        override lazy val itemConfId = "csv_change_conf_id"
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success change env name" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "csv"
        val fileInputInfo = CsvInfo(Set("csv.dat"), "ENV_TEST")
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success exist header" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "csv"
        val fileInputInfo = CsvInfo(Set("csv_h.dat"), header = true)
        override lazy val itemConfId = "csv_h"
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }
  }

  "NoConfFileToXxx for tsv" should {
    implicit val inArgs = TestArgs().toInputArgs

    "success simplePattern test" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "tsv"
        val fileInputInfo = TsvInfo(Set("tsv.dat"))
      }
      val result = fileToDf.run(Unit).collect
      result.foreach(println)
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success cnange conf id" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "tsv"
        override lazy val itemConfId = "tsv_change_conf_id"
        val fileInputInfo = TsvInfo(Set("tsv.dat"))
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success file path from componentId and change conf id" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fptest"
        val fileInputInfo = TsvInfo(Set("tsv.dat"))
        override lazy val itemConfId = "tsv_change_conf_id"
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success change env name" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "tsv"
        val fileInputInfo = TsvInfo(Set("tsv.dat"), "ENV_TEST")
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success drop double quote mode on" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "tsv"
        val fileInputInfo = TsvInfo(Set("tsv_strict.dat"), dropDoubleQuoteMode = true)
        override lazy val itemConfId = "tsv_h"
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success exist header" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "tsv"
        val fileInputInfo = TsvInfo(Set("tsv_h.dat"), header = true)
        override lazy val itemConfId = "tsv_h"
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }
  }

  "NoConfFileToXxx for vsv" should {
    implicit val inArgs = TestArgs().toInputArgs

    "success simplePattern test" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "vsv"
        val fileInputInfo = VsvInfo(Set("vsv.dat"))
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success cnange conf id" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "vsv"
        override lazy val itemConfId = "vsv_change_conf_id"
        val fileInputInfo = VsvInfo(Set("vsv.dat"))
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success file path from componentId and change conf id" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fptest"
        val fileInputInfo = VsvInfo(Set("vsv.dat"))
        override lazy val itemConfId = "vsv_change_conf_id"
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success change env name" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "vsv"
        val fileInputInfo = VsvInfo(Set("vsv.dat"), "ENV_TEST")
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success exist header" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "vsv"
        val fileInputInfo = VsvInfo(Set("vsv_h.dat"), header = true)
        override lazy val itemConfId = "vsv_h"
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }
  }

  "NoConfFileToXxx for ssv" should {
    implicit val inArgs = TestArgs().toInputArgs

    "success simplePattern test" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "csv"
        val fileInputInfo = SsvInfo(Set("ssv.dat"))
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success exist header" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "csv"
        val fileInputInfo = SsvInfo(Set("ssv_h.dat"), header = true)
        override lazy val itemConfId = "csv_h"
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }
  }

  "NoConfFileToXxx for fixed" should {
    implicit val inArgs = TestArgs().toInputArgs

    "success simplePattern test" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        val fileInputInfo = FixedInfo(Set("fixed.dat"))
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success cnange conf id" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        override lazy val itemConfId = "fixed_change_conf_id"
        val fileInputInfo = FixedInfo(Set("fixed.dat"))
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success file path from componentId and change conf id" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fptest"
        val fileInputInfo = FixedInfo(Set("fixed.dat"))
        override lazy val itemConfId = "fixed_change_conf_id"
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success change env name" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        val fileInputInfo = FixedInfo(Set("fixed.dat"), "ENV_TEST")
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success exist header" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        val fileInputInfo = FixedInfo(Set("fixed_h.dat"), header = true)
        override lazy val itemConfId = "fixed_h"
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success exist footer" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        val fileInputInfo = FixedInfo(Set("fixed_f.dat"), footer = true)
        override lazy val itemConfId = "fixed_f"
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success exist header and footer" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        val fileInputInfo = FixedInfo(Set("fixed_hf.dat"), header = true, footer = true)
        override lazy val itemConfId = "fixed_hf"
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    import Converter._
    "success withIndex test" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        val fileInputInfo = FixedInfo(Set("fixed.dat"), withIndex = true)
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
      result(0).getAs[String](SYSTEM_COLUMN_NAME.RECORD_INDEX) mustBe "0"
      result(1).getAs[String](SYSTEM_COLUMN_NAME.RECORD_INDEX) mustBe "1"
      result(2).getAs[String](SYSTEM_COLUMN_NAME.RECORD_INDEX) mustBe "2"
    }

    "success record length check test" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        val fileInputInfo = FixedInfo(Set("fixed.dat"), recordLengthCheck = true)
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
      result(0).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "false"
      result(1).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "false"
      result(2).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "false"
    }

    "success record length check illegal size test" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        val fileInputInfo = FixedInfo(Set("fixed_size_error.dat"), recordLengthCheck = true)
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 4
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
      result(0).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "false"
      result(1).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "true"
      result(2).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "true"
      result(3).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "true"
    }

    "success record length check illegal size and with index test" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        val fileInputInfo = FixedInfo(Set("fixed_size_error.dat"), withIndex = true, recordLengthCheck = true)
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 4
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
      result(0).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "false"
      result(0).getAs[String](SYSTEM_COLUMN_NAME.RECORD_INDEX) mustBe "0"
      result(1).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "true"
      result(1).getAs[String](SYSTEM_COLUMN_NAME.RECORD_INDEX) mustBe "1"
      result(2).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "true"
      result(2).getAs[String](SYSTEM_COLUMN_NAME.RECORD_INDEX) mustBe "2"
      result(3).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "true"
      result(3).getAs[String](SYSTEM_COLUMN_NAME.RECORD_INDEX) mustBe "3"
    }

    "success record length throw Exception by linebreak test" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        val fileInputInfo = FixedInfo(Set("fixed_size_error.dat"), recordLengthCheck = true, newLine = false)
      }
      try {
        fileToDf.run(Unit).collect
        fail
      } catch {
        case t: IllegalArgumentException => t.printStackTrace
      }
    }

    "success crlf test" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        val fileInputInfo = FixedInfo(Set("fixed_crlf.dat"), newLineCode = CRLF)
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }

    "success cr test" in {
      val fileToDf = new template.FileToDf with executor.Nothing {
        val componentId = "fixed"
        val fileInputInfo = FixedInfo(Set("fixed_cr.dat"), newLineCode = CR)
      }
      val result = fileToDf.run(Unit).collect
      result.size mustBe 3
      result(0).getAs[String]("item1") mustBe "a1"
      result(0).getAs[String]("item2") mustBe "bb1"
    }
  }

  "preFilter" should {
    implicit val inArgs = TestArgs().toInputArgs

    "be success" when {
      "simple pattern" in {
        val fileToDf = new template.FileToDf with executor.Nothing {
          val componentId = "fixed"
          val fileInputInfo = FixedInfo(
            Set("fixed.dat"),
            preFilter = (Seq("item1"), m => m("item1") == "a1"))
        }
        val result = fileToDf.run(Unit).collect
        result.size mustBe 1
        result(0).getAs[String]("item1") mustBe "a1"
        result(0).getAs[String]("item2") mustBe "bb1"
      }

      import Converter._
      "success withIndex test" in {
        val fileToDf = new template.FileToDf with executor.Nothing {
          val componentId = "fixed"
          val fileInputInfo = FixedInfo(Set("fixed.dat"), withIndex = true,
            preFilter = (Seq("item1"), m => m("item1") == "a1" || m("item1") == "a2"))
        }
        val result = fileToDf.run(Unit).collect
        result.size mustBe 2
        result(0).getAs[String]("item1") mustBe "a1"
        result(0).getAs[String]("item2") mustBe "bb1"
        result(0).getAs[String](SYSTEM_COLUMN_NAME.RECORD_INDEX) mustBe "0"
        result(1).getAs[String](SYSTEM_COLUMN_NAME.RECORD_INDEX) mustBe "1"
      }

      "success record length check test" in {
        val fileToDf = new template.FileToDf with executor.Nothing {
          val componentId = "fixed"
          val fileInputInfo = FixedInfo(Set("fixed.dat"), recordLengthCheck = true,
            preFilter = (Seq("item1"), m => m("item1") == "a1" || m("item1") == "a2"))
        }
        val result = fileToDf.run(Unit).collect
        result.size mustBe 2
        result(0).getAs[String]("item1") mustBe "a1"
        result(0).getAs[String]("item2") mustBe "bb1"
        result(0).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "false"
        result(1).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "false"
      }

      "success record length check illegal size test" in {
        val fileToDf = new template.FileToDf with executor.Nothing {
          val componentId = "fixed"
          val fileInputInfo = FixedInfo(Set("fixed_size_error.dat"), recordLengthCheck = true,
            preFilter = (Seq("item2"), m => m("item2") == "bb1" || m("item2") == "b2X"))
        }
        fileToDf.run(Unit).show
        val result = fileToDf.run(Unit).collect
        result.size mustBe 2
        result(0).getAs[String]("item1") mustBe "a1"
        result(0).getAs[String]("item2") mustBe "bb1"
        result(0).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "false"
        result(1).getAs[String](SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR) mustBe "true"
      }
    }
  }
}
