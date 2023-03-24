package d2k.common.fileConv

import com.snowflake.snowpark.functions._
import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.common.TestArgs
import d2k.common.df.executor.BinaryRecordConverter
import d2k.common.df.template._
import d2k.common.df.executor.Nothing
import d2k.common.df.FixedInfo
import spark.common.DfCtl.implicits._
import scala.util.Try
import d2k.common.df.executor.PqCommonColumnRemover
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class BinaryRecordTest extends WordSpec with MustMatchers with BeforeAndAfter {
   implicit val inArgs = TestArgs().toInputArgs

   "fixed with Binary Record" should {
   "be normal end" when {
      val binrecName = "binrec"
"MS932" in {
         val compo = new FileToDf with Nothing {
               val componentId = "fixed"

               override val fileInputInfo = FixedInfo(Set("fixed_binrec.dat"), itemConfId = "binrecPre", withBinaryRecord = binrecName)
            }
 val df = compo.run(Unit)
 val result = df.collect
result.foreach{ row =>row.getAs[String]("pre_data_div") + row.getAs[String]("pre_item1") + row.getAs[String]("pre_item2") mustBe new String (row.getAs[Array[Byte]](binrecName), "MS932").trim
}
 val converted = df ~> BinaryRecordConverter(binrecName, "binrecPost", "MS932")_
converted.collect.foreach{ row =>row.getAs[String]("pre_data_div") mustBe row.getAs[String]("data_div")
row.getAs[String]("pre_item1") mustBe row.getAs[String]("item1")
row.getAs[String]("pre_item2") mustBe row.getAs[String]("item2")
}
         }
"JEF" in {
         val fileToDf = new FileToDf with Nothing {
               val componentId = "fixed_jef_sample_pre"

               val fileInputInfo = FixedInfo(Set("org.dat"), "JEF_SAMPLE", newLine = false, charSet = "JEF", withBinaryRecord = binrecName)
            }
 val df = fileToDf.run(Unit)
 val converted = df ~> BinaryRecordConverter(binrecName, "fixed_jef_sample_post", "JEF")_
converted.collect.foreach{ row =>(1 to 26).map( d =>row.getAs[String](s"pre_item${ d }") mustBe row.getAs[String](s"item${ d }"))
}
         }
"JEF" when {
         "Parquet read and write" in {
            val fileToPq = new FileToPq with Nothing {
                  val componentId = "fixed_jef_sample_pre"

                  val fileInputInfo = FixedInfo(Set("org.dat"), "JEF_SAMPLE", newLine = false, charSet = "JEF", withBinaryRecord = binrecName)
               }
 val pqToDf = new PqToDf with Nothing {
                  val componentId = "fixed_jef_sample_pre"
               }
fileToPq.run(Unit)
 val df = pqToDf.run(Unit)
 val converted = df ~> BinaryRecordConverter(binrecName, "fixed_jef_sample_post", "JEF")_
converted.collect.foreach{ row =>(1 to 26).map( d =>row.getAs[String](s"pre_item${ d }") mustBe row.getAs[String](s"item${ d }"))
}
            }
         }
"Executor pattern" in {
         val compo = new FileToDf with Nothing {
               val componentId = "fixed"

               override val fileInputInfo = FixedInfo(Set("fixed_binrec.dat"), itemConfId = "binrecPre", withBinaryRecord = binrecName)
            }
 val df = compo.run(Unit)
 val result = df.collect
result.foreach{ row =>row.getAs[String]("pre_data_div") + row.getAs[String]("pre_item1") + row.getAs[String]("pre_item2") mustBe new String (row.getAs[Array[Byte]](binrecName), "MS932").trim
}
 val dfToDf = new DfToDf with BinaryRecordConverter {
               val binaryRecordName = binrecName

               val itemConfId = "binrecPost"

               val charEnc = "MS932"
            }
 val converted = df ~> dfToDf.run_
converted.collect.foreach{ row =>row.getAs[String]("pre_data_div") mustBe row.getAs[String]("data_div")
row.getAs[String]("pre_item1") mustBe row.getAs[String]("item1")
row.getAs[String]("pre_item2") mustBe row.getAs[String]("item2")
}
         }
      }
"normal end withIndex mode" when {
      val binrecName = "binrec"
"MS932" in {
         val compo = new FileToDf with Nothing {
               val componentId = "fixed"

               override val fileInputInfo = FixedInfo(Set("fixed_binrec.dat"), itemConfId = "binrecPre", withBinaryRecord = binrecName, 
withIndex = true)
            }
 val df = compo.run(Unit)
 val result = df.collect
result.foreach{ row =>row.getAs[String]("pre_data_div") + row.getAs[String]("pre_item1") + row.getAs[String]("pre_item2") mustBe new String (row.getAs[Array[Byte]](binrecName), "MS932").trim
}
 val converted = df ~> BinaryRecordConverter(binrecName, "binrecPost", "MS932")_
converted.collect.foreach{ row =>row.getAs[String]("pre_data_div") mustBe row.getAs[String]("data_div")
row.getAs[String]("pre_item1") mustBe row.getAs[String]("item1")
row.getAs[String]("pre_item2") mustBe row.getAs[String]("item2")
}
         }
"JEF" in {
         val fileToDf = new FileToDf with Nothing {
               val componentId = "fixed_jef_sample_pre"

               val fileInputInfo = FixedInfo(Set("org.dat"), "JEF_SAMPLE", newLine = false, charSet = "JEF", withBinaryRecord = binrecName, 
withIndex = true)
            }
Try{
            fileToDf.run(Unit)
fail
            }.failed.get.getMessage mustBe "JEF CharEnc is not supportted"
         }
      }
"normal end recordLengthCheck on" when {
      val binrecName = "binrec"
"MS932" in {
         val compo = new FileToDf with Nothing {
               val componentId = "fixed"

               override val fileInputInfo = FixedInfo(Set("fixed_binrec.dat"), itemConfId = "binrecPre", withBinaryRecord = binrecName, 
recordLengthCheck = true)
            }
 val df = compo.run(Unit)
 val result = df.collect
result.foreach{ row =>row.getAs[String]("pre_data_div") + row.getAs[String]("pre_item1") + row.getAs[String]("pre_item2") mustBe new String (row.getAs[Array[Byte]](binrecName), "MS932").trim
}
 val converted = df ~> BinaryRecordConverter(binrecName, "binrecPost", "MS932")_
converted.collect.foreach{ row =>row.getAs[String]("pre_data_div") mustBe row.getAs[String]("data_div")
row.getAs[String]("pre_item1") mustBe row.getAs[String]("item1")
row.getAs[String]("pre_item2") mustBe row.getAs[String]("item2")
}
         }
"JEF throw Exception" in {
         val fileToDf = new FileToDf with Nothing {
               val componentId = "fixed_jef_sample_pre"

               val fileInputInfo = FixedInfo(Set("org.dat"), "JEF_SAMPLE", newLine = false, charSet = "JEF", withBinaryRecord = binrecName, 
recordLengthCheck = true)
            }
Try{
            fileToDf.run(Unit)
fail
            }.failed.get.getMessage mustBe "JEF CharEnc is not supportted"
         }
      }
"normal end recordLengthCheck on and withIndex" when {
      val binrecName = "binrec"
"MS932" in {
         val compo = new FileToDf with Nothing {
               val componentId = "fixed"

               override val fileInputInfo = FixedInfo(Set("fixed_binrec.dat"), itemConfId = "binrecPre", withBinaryRecord = binrecName, 
recordLengthCheck = true, withIndex = true)
            }
 val df = compo.run(Unit)
 val result = df.collect
result.foreach{ row =>row.getAs[String]("pre_data_div") + row.getAs[String]("pre_item1") + row.getAs[String]("pre_item2") mustBe new String (row.getAs[Array[Byte]](binrecName), "MS932").trim
}
 val converted = df ~> BinaryRecordConverter(binrecName, "binrecPost", "MS932")_
converted.collect.foreach{ row =>row.getAs[String]("pre_data_div") mustBe row.getAs[String]("data_div")
row.getAs[String]("pre_item1") mustBe row.getAs[String]("item1")
row.getAs[String]("pre_item2") mustBe row.getAs[String]("item2")
}
         }
"JEF throw Exception" in {
         val fileToDf = new FileToDf with Nothing {
               val componentId = "fixed_jef_sample_pre"

               val fileInputInfo = FixedInfo(Set("org.dat"), "JEF_SAMPLE", newLine = false, charSet = "JEF", withBinaryRecord = binrecName, 
recordLengthCheck = true, withIndex = true)
            }
Try{
            fileToDf.run(Unit)
fail
            }.failed.get.getMessage mustBe "JEF CharEnc is not supportted"
         }
      }
   }

   "MCA0130071 test pattern" should {
   "be normal end" in {
      val binrecName = "binrec"
 val c1 = new FileToPq with PqCommonColumnRemover {
            val componentId = "fixed1"

            override val fileInputInfo = FixedInfo(Set("fixed_binrec.dat"), itemConfId = "binrecPreNoItem", withBinaryRecord = binrecName)
         }
 val c2 = new FileToPq with PqCommonColumnRemover {
            val componentId = "fixed2"

            override val fileInputInfo = FixedInfo(Set("fixed_binrec2.dat"), itemConfId = "binrecPreNoItem", withBinaryRecord = binrecName)
         }
 val c3 = new PqToDf with Nothing {
            val componentId = "fixed1"
         }
 val c4 = new PqToDf with Nothing {
            val componentId = "fixed2"
         }
c1.run(Unit)
c2.run(Unit)
 val df1 = c3.run(Unit)
 val df2 = c4.run(Unit)
 val df = df1.except(df2)
 val converted = df ~> BinaryRecordConverter(binrecName, "binrecPost", "MS932")_
 val row = converted.collect
row.size mustBe 1
row(0).getAs[String]("data_div") mustBe "D"
row(0).getAs[String]("item1") mustBe "a3"
row(0).getAs[String]("item2") mustBe "bb3"
      }
   }
}