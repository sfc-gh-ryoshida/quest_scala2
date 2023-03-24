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
import com.snowflake.snowpark.types._
import spark.common.SparkContexts.context
import context.implicits._
import com.snowflake.snowpark.Row
import spark.common.DbCtl
import scala.reflect.io.Directory
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class WriteFilePartitionTest extends WordSpec with MustMatchers with BeforeAndAfter {
   implicit val inArgs = TestArgs().toInputArgs

   val testData = (1 to 100).map( idx =>TestData("あ～", s"$idx")).toDF.repartition(10)

   "Fixed WriteFileTest" should {
   "be normal end." in {
      val writeFile = new WriteFile {
            val componentId = "paraWrite1"

            override val writeFileMode = partition.Fixed(5, 5)
         }
writeFile.writeFile(testData)
 val d = Directory(s"${ inArgs.baseOutputFilePath }/paraWrite1")
 val l = d.files.toList
l.size mustBe 10
 val recs = Source.fromInputStream(l(0).inputStream, "MS932").getLines.toList
recs.size mustBe 10
recs(0).getBytes("MS932").size mustBe 10
recs(0).contains("あ～") mustBe true
recs(1).contains("あ～") mustBe true
 val recs2 = Source.fromInputStream(l(1).inputStream, "MS932").getLines.toList
recs2.size mustBe 10
recs(0).getBytes("MS932").size mustBe 10
recs2(0).contains("あ～") mustBe true
recs2(1).contains("あ～") mustBe true
      }
"add Extention" in {
      val writeFile = new WriteFile {
            val componentId = "paraWrite1_2"

            override val writeFileMode = partition.Fixed(5, 5)

            override val writeFilePartitionExtention = "ext"
         }
writeFile.writeFile(testData)
Directory(s"${ inArgs.baseOutputFilePath }/paraWrite1_2").files.foreach(_.name.takeRight(4) mustBe ".ext")
      }
   }

   "Csv WriteFileTest" should {
   "be normal end. arg pattern" in {
      val writeFile = new WriteFile {
            val componentId = "paraWrite2"

            override val writeFileMode = partition.Csv("a")
         }
writeFile.writeFile(testData)
 val d = Directory(s"${ inArgs.baseOutputFilePath }/paraWrite2")
 val l = d.files.toList
l.size mustBe 10
 val recs = Source.fromInputStream(l(0).inputStream, "MS932").getLines.toList
recs.size mustBe 10
recs(0).contains("\"あ～\"") mustBe true
recs(0).contains(",") mustBe true
recs(1).contains("\"あ～\"") mustBe true
recs(1).contains(",") mustBe true
 val recs2 = Source.fromInputStream(l(1).inputStream, "MS932").getLines.toList
recs2.size mustBe 10
recs2(0).contains("\"あ～\"") mustBe true
recs2(0).contains(",") mustBe true
recs2(1).contains("\"あ～\"") mustBe true
recs2(1).contains(",") mustBe true
      }
"add Extention" in {
      val writeFile = new WriteFile {
            val componentId = "paraWrite2_2"

            override val writeFileMode = partition.Csv("a")

            override val writeFilePartitionExtention = "ext"
         }
writeFile.writeFile(testData)
Directory(s"${ inArgs.baseOutputFilePath }/paraWrite2_2").files.foreach(_.name.takeRight(4) mustBe ".ext")
      }
"be normal end. Csv with empty arg pattern" in {
      val writeFile = new WriteFile {
            val componentId = "writeFile21"

            override val writeFileMode = partition.Csv()
         }
writeFile.writeFile(testData)
 val d = Directory(s"${ inArgs.baseOutputFilePath }/writeFile21")
 val l = d.files.toList
l.size mustBe 10
 val recs = Source.fromInputStream(l(0).inputStream, "MS932").getLines.toList
recs.size mustBe 10
recs(0).contains("あ～") mustBe true
recs(0).contains(",") mustBe true
recs(1).contains("あ～") mustBe true
recs(1).contains(",") mustBe true
 val recs2 = Source.fromInputStream(l(1).inputStream, "MS932").getLines.toList
recs2.size mustBe 10
recs2(0).contains("あ～") mustBe true
recs2(0).contains(",") mustBe true
recs2(1).contains("あ～") mustBe true
recs2(1).contains(",") mustBe true
      }
   }

   "Tsv WriteFileTest" should {
   "be normal end." in {
      val writeFile = new WriteFile {
            val componentId = "paraWriteTsv"

            override val writeFileMode = partition.Tsv
         }
writeFile.writeFile(testData)
 val d = Directory(s"${ inArgs.baseOutputFilePath }/paraWriteTsv")
 val l = d.files.toList
l.size mustBe 10
 val recs = Source.fromInputStream(l(0).inputStream, "MS932").getLines.toList
recs.size mustBe 10
recs(0).contains("あ～") mustBe true
recs(0).contains("\t") mustBe true
recs(1).contains("あ～") mustBe true
recs(1).contains("\t") mustBe true
 val recs2 = Source.fromInputStream(l(1).inputStream, "MS932").getLines.toList
recs2.size mustBe 10
recs2(0).contains("あ～") mustBe true
recs2(0).contains("\t") mustBe true
recs2(1).contains("あ～") mustBe true
recs2(1).contains("\t") mustBe true
      }
   }

   "writeCharEncoding by UTF-8" should {
   "csv" in {
      val writeFile = new WriteFile {
            val componentId = "paraWriteCsvByUTF8"

            override val writeFileMode = partition.Csv

            override val writeCharEncoding = "UTF-8"
         }
writeFile.writeFile(testData)
 val d = Directory(s"${ inArgs.baseOutputFilePath }/${ writeFile.componentId }")
 val l = d.files.toList
l.size mustBe 10
 val recs = Source.fromInputStream(l(0).inputStream, "UTF-8").getLines.toList
recs.size mustBe 10
recs(0).contains("あ～") mustBe true
recs(0).contains(",") mustBe true
recs(1).contains("あ～") mustBe true
recs(1).contains(",") mustBe true
 val recs2 = Source.fromInputStream(l(1).inputStream, "UTF-8").getLines.toList
recs2.size mustBe 10
recs2(0).contains("あ～") mustBe true
recs2(0).contains(",") mustBe true
recs2(1).contains("あ～") mustBe true
recs2(1).contains(",") mustBe true
      }
"tsv" in {
      val writeFile = new WriteFile {
            val componentId = "paraWriteTsvByUTF8"

            override val writeFileMode = partition.Tsv

            override val writeCharEncoding = "UTF-8"
         }
writeFile.writeFile(testData)
 val d = Directory(s"${ inArgs.baseOutputFilePath }/${ writeFile.componentId }")
 val l = d.files.toList
l.size mustBe 10
 val recs = Source.fromInputStream(l(0).inputStream, "UTF-8").getLines.toList
recs.size mustBe 10
recs(0).contains("あ～") mustBe true
recs(0).contains("\t") mustBe true
recs(1).contains("あ～") mustBe true
recs(1).contains("\t") mustBe true
 val recs2 = Source.fromInputStream(l(1).inputStream, "UTF-8").getLines.toList
recs2.size mustBe 10
recs2(0).contains("あ～") mustBe true
recs2(0).contains("\t") mustBe true
recs2(1).contains("あ～") mustBe true
recs2(1).contains("\t") mustBe true
      }
   }
}