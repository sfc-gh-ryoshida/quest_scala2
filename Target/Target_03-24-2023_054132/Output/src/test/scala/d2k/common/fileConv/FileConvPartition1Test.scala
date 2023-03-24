package d2k.common.fileConv

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.common.df.CsvInfo
import d2k.common.TestArgs
import scala.util.Try
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class FileConvPartition1Test extends WordSpec with MustMatchers with BeforeAndAfter {
   System.setProperty("spark.cores.max", "16")

   implicit val inArgs = TestArgs().toInputArgs

   "FileConv variable File Partition number" should {
   "equal core number" in {
      val compoId = "res"
 val fileNames = (1 to 9).map( s =>s"csv${ s }.dat").toSet
 val fc = new FileConv (compoId, CsvInfo(fileNames), compoId)
 val df = fc.makeDf
 val result = df.collect
df.rdd.partitions.size mustBe 9
      }
   }
}