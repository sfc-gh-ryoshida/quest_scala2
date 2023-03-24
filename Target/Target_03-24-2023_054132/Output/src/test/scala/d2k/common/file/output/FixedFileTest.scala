package d2k.common.file.output

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts.context
import context.implicits._
import scala.reflect.io.Directory
import spark.common.SparkContexts
import scala.io.Source
import d2k.common.TestArgs
import scala.reflect.io.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.BytesWritable
import scala.util.Try
import spark.common.DfCtl
import DfCtl._
import DfCtl.implicits._
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class TestData (a: String, b: String)
case class TestData2 (a: String, b: String, c: String, d: String, e: String, f: String, g: String)
case class TestData3 (z: String, b: String, x: String, d: String, e: String, y: String, g: String)
 class FixedFileTest extends WordSpec with MustMatchers with BeforeAndAfter {
   implicit val inArgs = TestArgs().toInputArgs

   val df = SparkContexts.sc.makeRDD(Seq(TestData("aaa", "bbb"), TestData("ccccc", "ddddd"))).toDF

   val dfemptydata = SparkContexts.sc.makeRDD(Seq(TestData("", "bbb"), TestData("ccccc", ""))).toDF

   val dfnulldata = SparkContexts.sc.makeRDD(Seq(TestData(null, "bbb"), TestData("ccccc", null))).toDF

   val dfkanjidata = SparkContexts.sc.makeRDD(Seq(TestData("あい", "うえお"), TestData("かきくけこ", "さしすせそ"))).toDF

   val outputDir = "test/dev/data/output2"

   def mkFilePath(fileName: String) = s"${ outputDir }/${ fileName }"

   "writeSingle_MS932" should {
   "be normal end. pattern1" in {
      new FixedFile (mkFilePath("writeSingle_normal1")).writeSingle_MS932(Seq(2, 6))(df)
 val lines = Source.fromFile(mkFilePath("writeSingle_normal1")).getLines.toList
lines(0) mustBe "aabbb   "
lines(1) mustBe "ccddddd "
      }
"be normal end. pattern2" in {
      new FixedFile (mkFilePath("writeSingle_normal2")).writeSingle_MS932(Seq(3, 3))(df)
 val lines = Source.fromFile(mkFilePath("writeSingle_normal2")).getLines.toList
lines(0) mustBe "aaabbb"
lines(1) mustBe "cccddd"
      }
"be normal end. pattern3" in {
      new FixedFile (mkFilePath("writeSingle_normal3")).writeSingle_MS932(Seq(5, 5))(df)
 val lines = Source.fromFile(mkFilePath("writeSingle_normal3")).getLines.toList
lines(0) mustBe "aaa  bbb  "
lines(1) mustBe "cccccddddd"
      }
"be normal end. no data" in {
      new FixedFile (mkFilePath("writeSingle_normal_nodata4")).writeSingle_MS932(Seq(2, 6))(df.filter(df("a") === "xx"))
 val lines = Source.fromFile(mkFilePath("writeSingle_normal_nodata4")).getLines.toList
lines.size mustBe 0
      }
"be normal end. empty data" in {
      new FixedFile (mkFilePath("writeSingle_normal5")).writeSingle_MS932(Seq(2, 6))(dfemptydata)
 val lines = Source.fromFile(mkFilePath("writeSingle_normal5")).getLines.toList
lines(0) mustBe "  bbb   "
lines(1) mustBe "cc      "
      }
"be normal end. null data" in {
      new FixedFile (mkFilePath("writeSingle_normal6")).writeSingle_MS932(Seq(2, 6))(dfnulldata)
 val lines = Source.fromFile(mkFilePath("writeSingle_normal6")).getLines.toList
lines(0) mustBe "  bbb   "
lines(1) mustBe "cc      "
      }
"be normal end. set size zero" in {
      new FixedFile (mkFilePath("writeSingle_normal7")).writeSingle_MS932(Seq(0, 0))(df)
 val lines = Source.fromFile(mkFilePath("writeSingle_normal7")).getLines.toList
lines(0) mustBe ""
lines(1) mustBe ""
      }
"be normal end. kanji pattern1" in {
      new FixedFile (mkFilePath("writeSingle_normal8")).writeSingle_MS932(Seq(4, 6))(dfkanjidata)
 val lines = Source.fromFile(mkFilePath("writeSingle_normal8"), "MS932").getLines.toList
lines(0) mustBe "あいうえお"
lines(1) mustBe "かきさしす"
      }
"be normal end. kanji pattern2" in {
      new FixedFile (mkFilePath("writeSingle_normal9")).writeSingle_MS932(Seq(10, 10))(dfkanjidata)
 val lines = Source.fromFile(mkFilePath("writeSingle_normal9"), "MS932").getLines.toList
lines(0) mustBe "あい      うえお    "
lines(1) mustBe "かきくけこさしすせそ"
      }
"writeSingle" when {
      "partitionColumns" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileTest/ptt1_1"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new FixedFile (fileName, Seq("a", "b")).writeSingle_MS932(Seq(1, 1, 1))(df)
('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>s"${ fileName }/a=${ x }/b=${ cnt }/0")).foreach{ path =>withClue(path){Path(path).exists mustBe true}
}
         }
"with partitionColumns add Extention" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileTest/ptt1_2"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new FixedFile (fileName, Seq("a", "b"), "ext").writeSingle_MS932(Seq(1, 1, 1))(df)
('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>s"${ fileName }/a=${ x }/b=${ cnt }/0.ext")).foreach{ path =>withClue(path){Path(path).exists mustBe true}
}
         }
      }
"writePartition" when {
      "partitionColumns" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileTest/ptt2_1"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new FixedFile (fileName, Seq("a", "b")).writePartition_MS932(Seq(1, 1, 1))(df)
Path(s"${ fileName }/a=a/b=1/0").exists mustBe true
Path(s"${ fileName }/a=a/b=2/0").exists mustBe true
Path(s"${ fileName }/a=a/b=3/1").exists mustBe true
Path(s"${ fileName }/a=b/b=1/1").exists mustBe true
Path(s"${ fileName }/a=b/b=2/2").exists mustBe true
Path(s"${ fileName }/a=b/b=3/2").exists mustBe true
Path(s"${ fileName }/a=c/b=1/3").exists mustBe true
Path(s"${ fileName }/a=c/b=2/3").exists mustBe true
Path(s"${ fileName }/a=c/b=3/3").exists mustBe true
         }
"add Extention" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileTest/ptt2_2"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new FixedFile (fileName, writeFilePartitionExtention = "ext").writePartition_MS932(Seq(1, 1, 1))(df)
Path(s"${ fileName }/0.ext").exists mustBe true
Path(s"${ fileName }/0.ext").exists mustBe true
Path(s"${ fileName }/1.ext").exists mustBe true
Path(s"${ fileName }/1.ext").exists mustBe true
Path(s"${ fileName }/2.ext").exists mustBe true
Path(s"${ fileName }/2.ext").exists mustBe true
Path(s"${ fileName }/3.ext").exists mustBe true
Path(s"${ fileName }/3.ext").exists mustBe true
Path(s"${ fileName }/3.ext").exists mustBe true
         }
"partitionColumns add Extention" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileTest/ptt2_3"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new FixedFile (fileName, Seq("a", "b"), writeFilePartitionExtention = "ext").writePartition_MS932(Seq(1, 1, 1))(df)
Path(s"${ fileName }/a=a/b=1/0.ext").exists mustBe true
Path(s"${ fileName }/a=a/b=2/0.ext").exists mustBe true
Path(s"${ fileName }/a=a/b=3/1.ext").exists mustBe true
Path(s"${ fileName }/a=b/b=1/1.ext").exists mustBe true
Path(s"${ fileName }/a=b/b=2/2.ext").exists mustBe true
Path(s"${ fileName }/a=b/b=3/2.ext").exists mustBe true
Path(s"${ fileName }/a=c/b=1/3.ext").exists mustBe true
Path(s"${ fileName }/a=c/b=2/3.ext").exists mustBe true
Path(s"${ fileName }/a=c/b=3/3.ext").exists mustBe true
         }
      }
"writeSequence" ignore {
      val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileSequence/pt1"
 val df = ('a' to 'c').map( x =>Test1(s"あ${ x }", 1, x.toString)).toDF
new FixedFile (fileName, Seq("a", "b")).writeSequence_MS932(Seq(3, 1, 2))(df)
 val result = SparkContexts.sc.sequenceFile(fileName, NullWritable.get.getClass, Test1.getClass).map(_.toString).collect.sorted
result(0) mustBe "((null),82 a0 61 31 61 20)"
result(1) mustBe "((null),82 a0 62 31 62 20)"
result(2) mustBe "((null),82 a0 63 31 63 20)"
      }
   }

   "writeHdfs" should {
   "be normal end" in {
      val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileHdfs/pt1"
 val df = ('a' to 'c').map( x =>Test1(s"あ${ x }", 1, x.toString)).toDF
new FixedFile (fileName).writeHdfs_MS932(Seq(4, 2, 3))(df)
 val result = SparkContexts.context.read.text(fileName).collect
result(0).getAs[String](0) mustBe "あa 1 a  "
result(1).getAs[String](0) mustBe "あb 1 b  "
result(2).getAs[String](0) mustBe "あc 1 c  "
      }
"empty check" in {
      val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileHdfs/pt1-1"
 val df = ('a' to 'c').map( x =>Test1("", 1, "")).toDF
new FixedFile (fileName).writeHdfs_MS932(Seq(4, 2, 3))(df)
 val result = SparkContexts.context.read.text(fileName).collect
result(0).getAs[String](0) mustBe "    1    "
result(1).getAs[String](0) mustBe "    1    "
result(2).getAs[String](0) mustBe "    1    "
      }
"null check" in {
      val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileHdfs/pt1-2"
 val df = ('a' to 'c').map( x =>Test1(null, 1, null)).toDF
new FixedFile (fileName).writeHdfs_MS932(Seq(4, 2, 3))(df)
 val result = SparkContexts.context.read.text(fileName).collect
result(0).getAs[String](0) mustBe "    1    "
result(1).getAs[String](0) mustBe "    1    "
result(2).getAs[String](0) mustBe "    1    "
      }
"writeHdfs partition write mode" in {
      val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileHdfs/pt2"
 val df = ('a' to 'c').map( x =>Test1(s"あ${ x }", 1, x.toString)).toDF
new FixedFile (fileName, Seq("a", "b")).writeHdfs_MS932(Seq(3))(df)
 val result = SparkContexts.context.read.text(fileName).select("value").sort("value").collect
result(0).getAs[String](0) mustBe "a  "
result(1).getAs[String](0) mustBe "b  "
result(2).getAs[String](0) mustBe "c  "
      }
"writeHdfs partition write mode. null test" in {
      val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileHdfs/pt2"
 val df = ('a' to 'c').map( x =>Test1(s"あ${ x }", 1, null)).toDF
new FixedFile (fileName, Seq("a", "b")).writeHdfs_MS932(Seq(3))(df)
 val result = SparkContexts.context.read.text(fileName).select("value").sort("value").collect
result(0).getAs[String](0) mustBe "   "
result(1).getAs[String](0) mustBe "   "
result(2).getAs[String](0) mustBe "   "
      }
"many columns" when {
      "sorted column name" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileHdfs/pt3"
 val dfx = ('a' to 'c').map( x =>TestData2(s"あ${ x }", "1", "2", "3", "4", "5", "6")).toDF
new FixedFile (fileName, Seq("b", "d", "f")).writeHdfs_MS932(Seq(4, 1, 2, 3))(dfx)
 val result = SparkContexts.context.read.text(fileName).collect
result(0).getAs[String](0) mustBe "あa 24 6  "
result(1).getAs[String](0) mustBe "あb 24 6  "
result(2).getAs[String](0) mustBe "あc 24 6  "
         }
"random column name" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/fixedFileHdfs/pt3"
 val dfx = ('a' to 'c').map( x =>TestData3(s"あ${ x }", "1", "2", "3", "4", "5", "6")).toDF
new FixedFile (fileName, Seq("e", "g", "b", "d")).writeHdfs_MS932(Seq(4, 1, 2))(dfx)
 val result = SparkContexts.context.read.text(fileName).collect
result(0).getAs[String](0) mustBe "あa 25 "
result(1).getAs[String](0) mustBe "あb 25 "
result(2).getAs[String](0) mustBe "あc 25 "
         }
      }
   }
}