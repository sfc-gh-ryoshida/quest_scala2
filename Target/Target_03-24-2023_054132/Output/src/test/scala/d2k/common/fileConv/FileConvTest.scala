package d2k.common.fileConv

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.common.df.CsvInfo
import d2k.common.TestArgs
import scala.util.Try
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class FileConvTest extends WordSpec with MustMatchers with BeforeAndAfter {
   implicit val inArgs = TestArgs().toInputArgs

   "FileConv" should {
   "file not exists mkEmptyDfWhenFileNotExists is false(default)" in {
      val compoId = "csv"
 val fc = new FileConv (compoId, CsvInfo(Set("csv.da")), compoId)
 val df = try
            {fc.makeDf
fail}
         catch {
            case ex:org.apache.spark.sql.AnalysisException => ex.getMessage.startsWith("Path does not exist:") mustBe true
         }
      }
"file exists mkEmptyDfWhenFileNotExists is false(default)" in {
      val compoId = "csv"
 val fc = new FileConv (compoId, CsvInfo(Set("csv.dat")), compoId)
 val df = fc.makeDf
df.count mustBe 3
df.schema.map(_.name).mkString(",") mustBe "item1,item2,ROW_ERR,ROW_ERR_MESSAGE"
      }
"file not exists mkEmptyDfWhenFileNotExists is true" in {
      val compoId = "csv"
 val fc = new FileConv (compoId, CsvInfo(Set("csv.da")), compoId, true)
 val df = fc.makeDf
df.count mustBe 0
df.schema.map(_.name).mkString(",") mustBe "item1,item2,ROW_ERR,ROW_ERR_MESSAGE"
      }
"file exists mkEmptyDfWhenFileNotExists is true" in {
      val compoId = "csv"
 val fc = new FileConv (compoId, CsvInfo(Set("csv.dat")), compoId, true)
 val df = fc.makeDf
df.count mustBe 3
df.schema.map(_.name).mkString(",") mustBe "item1,item2,ROW_ERR,ROW_ERR_MESSAGE"
      }
"read UTF-8 data" in {
      val compoId = "csv"
 val fc = new FileConv (compoId, CsvInfo(Set("csv_utf8.dat"), charSet = "UTF-8"), compoId, true)
 val df = fc.makeDf
df.count mustBe 3
 val r = df.collect
r(0).getAs[String]("item1") mustBe "あ"
r(0).getAs[String]("item2") mustBe "う1"
r(1).getAs[String]("item1") mustBe "い"
r(1).getAs[String]("item2") mustBe "え2"
r(2).getAs[String]("item1") mustBe ""
r(2).getAs[String]("item2") mustBe ""
      }
   }

   "read from Resource" should {
   "success read" in {
      val compoId = "res"
 val fc = new FileConv (compoId, CsvInfo(Set("res.dat")), compoId)
 val result = fc.makeDf.collect
{
         val r = result(0)
r.getAs[String]("xxx1") mustBe "res1"
r.getAs[String]("xxx2") mustBe "bb1"
         }
{
         val r = result(1)
r.getAs[String]("xxx1") mustBe "res2"
r.getAs[String]("xxx2") mustBe "bb2"
         }
      }
   }

   "FileConv variable File Partition number" should {
   "equal core number" in {
      val compoId = "res"
 val fileNames = (1 to 9).map( s =>s"csv${ s }.dat").toSet
 val fc = new FileConv (compoId, CsvInfo(fileNames), compoId)
 val df = fc.makeDf
 val result = df.collect
df.rdd.partitions.size mustBe 3
      }
   }
}