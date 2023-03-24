package d2k.common.file.output

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts.context
import context.implicits._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.Row
import d2k.common.InputArgs
import com.snowflake.snowpark.types._
import spark.common.SparkContexts
import scala.io.Source
import d2k.common.TestArgs
import scala.reflect.io.Path
import org.apache.hadoop.io.NullWritable
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class Test1 (a: String, b: Int, c: String)
case class Test2 (str1: String, str2: String, sqlDate: java.sql.Date, sqlTimestamp: java.sql.Timestamp, decimal: BigDecimal)
 class VariableFileTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "VariableFile" should {
   implicit val inArgs = TestArgs().toInputArgs
"mkOutputStr Wrap Double Quate normal end" in {
      val wrapDoubleQuote = true
VariableFile.mkOutputStr(Row("key1", "あああ～"), ",", wrapDoubleQuote, "\"") mustBe """"key1","あああ～""""
VariableFile.mkOutputStr(Row("key1", "a\"b"), ",", wrapDoubleQuote, "\"") mustBe """"key1","a""b""""
VariableFile.mkOutputStr(Row("key1", ""), ",", wrapDoubleQuote, "\"") mustBe """"key1","""""
VariableFile.mkOutputStr(Row("key1", "\""), ",", wrapDoubleQuote, "\"") mustBe """"key1","""""""
VariableFile.mkOutputStr(Row("key1", "\"\""), ",", wrapDoubleQuote, "\"") mustBe """"key1","""""""""
VariableFile.mkOutputStr(Row("key1", null), ",", wrapDoubleQuote, "\"") mustBe """"key1","""""
      }
"mkOutputStr UnWrap Double Quate normal end" in {
      val wrapDoubleQuote = false
VariableFile.mkOutputStr(Row("key1", "あああ～"), ",", wrapDoubleQuote, "\"") mustBe """key1,あああ～"""
VariableFile.mkOutputStr(Row("key1", "a\"b"), ",", wrapDoubleQuote, "\"") mustBe """key1,a"b"""
VariableFile.mkOutputStr(Row("key1", ""), ",", wrapDoubleQuote, "\"") mustBe """key1,"""
VariableFile.mkOutputStr(Row("key1", "\""), ",", wrapDoubleQuote, "\"") mustBe """key1,""""
VariableFile.mkOutputStr(Row("key1", "\"\""), ",", wrapDoubleQuote, "\"") mustBe """key1,"""""
VariableFile.mkOutputStr(Row("key1", null), ",", wrapDoubleQuote, "\"") mustBe """key1,"""
      }
"writeSingleCsvWithDoubleQuote_MS932. 1 wrapped pattern. wrapDoubleQuote = true" in {
      implicit val inArgs = TestArgs().toInputArgs
 val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/1"
 val structType = StructType(Seq(
StructField("A", StringType), StructField("B", StringType), StructField("C", StringType), StructField("D", DecimalType(5, 0))))
 val df = context.createDataFrame(SparkContexts.sc.makeRDD(
Seq(Row("a1", "b1", "c1", BigDecimal(100)), Row("a2", "b\"2", "c2", BigDecimal(200)), Row(null, null, null, null))), structType)
new VariableFile (fileName, true, "\"", "MS932").writeSingleCsvWithDoubleQuote(Set("B"))(df)
 val result = Source.fromFile(fileName).getLines.toList
result(0) mustBe """a1,"b1",c1,100"""
result(1) mustBe """a2,"b""2",c2,200"""
result(2) mustBe ""","",,"""
      }
"writeSingleCsvWithDoubleQuote_MS932. 1 wrapped pattern. wrapDoubleQuote = false" in {
      implicit val inArgs = TestArgs().toInputArgs
 val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/1-2"
 val structType = StructType(Seq(
StructField("A", StringType), StructField("B", StringType), StructField("C", StringType), StructField("D", DecimalType(5, 0))))
 val df = context.createDataFrame(SparkContexts.sc.makeRDD(
Seq(Row("a1", "b1", "c1", BigDecimal(100)), Row("a2", "b\"2", "c2", BigDecimal(200)), Row(null, null, null, null))), structType)
new VariableFile (fileName, false, "\"", "MS932").writeSingleCsvWithDoubleQuote(Set("B"))(df)
 val result = Source.fromFile(fileName).getLines.toList
result(0) mustBe """a1,"b1",c1,100"""
result(1) mustBe """a2,"b""2",c2,200"""
result(2) mustBe """,,,"""
      }
"writeSingleCsvWithDoubleQuote_MS932. no wrapped pattern. wrapDoubleQuote = true" in {
      implicit val inArgs = TestArgs().toInputArgs
 val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/no"
 val structType = StructType(Seq(
StructField("A", StringType), StructField("B", StringType), StructField("C", StringType), StructField("D", DecimalType(5, 0))))
 val df = context.createDataFrame(SparkContexts.sc.makeRDD(
Seq(Row("a1", "b1", "c1", BigDecimal(100)), Row("a2", "b\"2", "c2", BigDecimal(200)), Row(null, null, null, null))), structType)
new VariableFile (fileName, true, "\"", "MS932").writeSingleCsvWithDoubleQuote(Set.empty[String])(df)
 val result = Source.fromFile(fileName).getLines.toList
result(0) mustBe """a1,b1,c1,100"""
result(1) mustBe """a2,b"2,c2,200"""
result(2) mustBe """,,,"""
      }
"writeSingleCsvWithDoubleQuote_MS932. no wrapped pattern. wrapDoubleQuote = false" in {
      implicit val inArgs = TestArgs().toInputArgs
 val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/no1"
 val structType = StructType(Seq(
StructField("A", StringType), StructField("B", StringType), StructField("C", StringType), StructField("D", DecimalType(5, 0))))
 val df = context.createDataFrame(SparkContexts.sc.makeRDD(
Seq(Row("a1", "b1", "c1", BigDecimal(100)), Row("a2", "b\"2", "c2", BigDecimal(200)), Row(null, null, null, null))), structType)
new VariableFile (fileName, false, "\"", "MS932").writeSingleCsvWithDoubleQuote(Set.empty[String])(df)
 val result = Source.fromFile(fileName).getLines.toList
result(0) mustBe """a1,b1,c1,100"""
result(1) mustBe """a2,b"2,c2,200"""
result(2) mustBe """,,,"""
      }
"writeSingleCsvWithDoubleQuote_MS932. all wrapped pattern. wrapDoubleQuote = true" in {
      implicit val inArgs = TestArgs().toInputArgs
 val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/2"
 val structType = StructType(Seq(
StructField("A", StringType), StructField("B", StringType), StructField("C", StringType), StructField("D", DecimalType(5, 0))))
 val df = context.createDataFrame(SparkContexts.sc.makeRDD(
Seq(Row("a1", "b1", "c1", BigDecimal(100)), Row("a2", "b\"2", "c2", BigDecimal(200)), Row(null, null, null, null))), structType)
new VariableFile (fileName, true, "\"", "MS932").writeSingleCsvWithDoubleQuote(Set("D", "C", "B", "A"))(df)
 val result = Source.fromFile(fileName).getLines.toList
result(0) mustBe """"a1","b1","c1","100""""
result(1) mustBe """"a2","b""2","c2","200""""
result(2) mustBe """"","","","""""
      }
"writeSingleCsvWithDoubleQuote_MS932. all wrapped pattern. wrapDoubleQuote = false" in {
      implicit val inArgs = TestArgs().toInputArgs
 val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/2-2"
 val structType = StructType(Seq(
StructField("A", StringType), StructField("B", StringType), StructField("C", StringType), StructField("D", DecimalType(5, 0))))
 val df = context.createDataFrame(SparkContexts.sc.makeRDD(
Seq(Row("a1", "b1", "c1", BigDecimal(100)), Row("a2", "b\"2", "c2", BigDecimal(200)), Row(null, null, null, null))), structType)
new VariableFile (fileName, false, "\"", "MS932").writeSingleCsvWithDoubleQuote(Set("D", "C", "B", "A"))(df)
 val result = Source.fromFile(fileName).getLines.toList
result(0) mustBe """"a1","b1","c1","100""""
result(1) mustBe """"a2","b""2","c2","200""""
result(2) mustBe """,,,"""
      }
"writeSingleCsvWithDoubleQuote_MS932. 5 wrapped pattern. wrapDoubleQuote = true" in {
      implicit val inArgs = TestArgs().toInputArgs
 val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/3"
 val structType = StructType(Seq(
StructField("A", StringType), StructField("B", StringType), StructField("C", StringType), 
StructField("D", StringType), StructField("E", StringType), StructField("F", StringType), StructField("G", DecimalType(5, 0))))
 val df = context.createDataFrame(SparkContexts.sc.makeRDD(
Seq(Row("a1", "b1", "c1", "d1", "e1", "f1", BigDecimal(100)), Row("a2", "b\"2", "c2", "d2", "e2", "f2", BigDecimal(200)), 
Row(null, null, null, null, null, null, null))), structType)
new VariableFile (fileName, true, "\"", "MS932").writeSingleCsvWithDoubleQuote(Set("A", "B", "C", "D", "E", "G"))(df)
 val result = Source.fromFile(fileName).getLines.toList
result(0) mustBe """"a1","b1","c1","d1","e1",f1,"100""""
result(1) mustBe """"a2","b""2","c2","d2","e2",f2,"200""""
result(2) mustBe """"","","","","",,"""""
      }
"writeSingleCsvWithDoubleQuote_MS932. 5 wrapped pattern. wrapDoubleQuote = false" in {
      implicit val inArgs = TestArgs().toInputArgs
 val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/3-2"
 val structType = StructType(Seq(
StructField("A", StringType), StructField("B", StringType), StructField("C", StringType), 
StructField("D", StringType), StructField("E", StringType), StructField("F", StringType), StructField("G", DecimalType(5, 0))))
 val df = context.createDataFrame(SparkContexts.sc.makeRDD(
Seq(Row("a1", "b1", "c1", "d1", "e1", "f1", BigDecimal(100)), Row("a2", "b\"2", "c2", "d2", "e2", "f2", BigDecimal(200)), 
Row(null, null, null, null, null, null, null))), structType)
new VariableFile (fileName, false, "\"", "MS932").writeSingleCsvWithDoubleQuote(Set("A", "B", "C", "D", "E", "G"))(df)
 val result = Source.fromFile(fileName).getLines.toList
result(0) mustBe """"a1","b1","c1","d1","e1",f1,"100""""
result(1) mustBe """"a2","b""2","c2","d2","e2",f2,"200""""
result(2) mustBe """,,,,,,"""
      }
"writeSingle" when {
      "partitionColumns" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/ptt1_1"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new VariableFile (fileName, false, "\"", "MS932", Seq("a", "b")).writeSingle(",")(df)
('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>s"${ fileName }/a=${ x }/b=${ cnt }/0")).foreach{ path =>withClue(path){Path(path).exists mustBe true}
}
         }
"partitionColumns add Extention" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/ptt1_2"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new VariableFile (fileName, false, "\"", "MS932", Seq("a", "b"), "ext").writeSingle(",")(df)
('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>s"${ fileName }/a=${ x }/b=${ cnt }/0.ext")).foreach{ path =>withClue(path){Path(path).exists mustBe true}
}
         }
      }
"writeSingleCsvWithDoubleQuote_MS932" when {
      "partitionColumns" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/ptt2_1"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new VariableFile (fileName, false, "MS932", "\"", Seq("a", "b")).writeSingleCsvWithDoubleQuote(Set("c"))(df)
('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>s"${ fileName }/a=${ x }/b=${ cnt }/0")).foreach{ path =>withClue(path){Path(path).exists mustBe true}
}
         }
"partitionColumns add Extention" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/ptt2_2"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new VariableFile (fileName, false, "MS932", "\"", Seq("a", "b"), "ext").writeSingleCsvWithDoubleQuote(Set("c"))(df)
('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>s"${ fileName }/a=${ x }/b=${ cnt }/0.ext")).foreach{ path =>withClue(path){Path(path).exists mustBe true}
}
         }
      }
"writePartition" when {
      "partitionColumns" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/ptt3_1"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new VariableFile (fileName, false, "\"", "MS932", Seq("a", "b")).writePartition(",")(df)
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
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/ptt3_2"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new VariableFile (fileName, false, "\"", "MS932", writeFilePartitionExtention = "ext").writePartition(",")(df)
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
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/ptt3_3"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new VariableFile (fileName, false, "\"", "MS932", Seq("a", "b"), "ext").writePartition(",")(df)
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
"writePartitionCsvWithDoubleQuote_MS932" when {
      "partitionColumns" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/ptt4_1"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new VariableFile (fileName, false, "\"", "MS932", Seq("a", "b")).writePartitionCsvWithDoubleQuote(Set("c"))(df)
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
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/ptt4_2"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new VariableFile (fileName, false, "\"", "MS932", writeFilePartitionExtention = "ext").writePartitionCsvWithDoubleQuote(Set("c"))(df)
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
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileTest/ptt4_3"
 val df = ('a' to 'c').flatMap( x =>(1 to 3).map( cnt =>Test1(x.toString, cnt, cnt.toString))).toDF
new VariableFile (fileName, false, "\"", "MS932", Seq("a", "b"), "ext").writePartitionCsvWithDoubleQuote(Set("c"))(df)
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
"writeHdfs" when {
      "csv" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileHdfs/pt1"
 val df = ('a' to 'c').map( x =>Test1(s"あ${ x }", 1, x.toString)).toDF
new VariableFile (fileName, true, "", "UTF-8").writeHdfs(",")(df)
 val result = SparkContexts.context.read.csv(fileName).collect
result(0).getAs[String](0) mustBe "あa"
result(0).getAs[String](1) mustBe "1"
result(0).getAs[String](2) mustBe "a"
         }
"csv without double quote" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileHdfs/pt2"
 val df = ('a' to 'c').map( x =>Test1(s"あ${ x }", 1, x.toString)).toDF
new VariableFile (fileName, false, "", "UTF-8").writeHdfs(",")(df)
 val result = SparkContexts.context.read.csv(fileName).collect
result(0).getAs[String](0) mustBe "あa"
result(0).getAs[String](1) mustBe "1"
result(0).getAs[String](2) mustBe "a"
         }
"trim無し確認" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileHdfs/pt3"
 val df = ('a' to 'c').map( x =>Test1(s"   あ${ x }  ", 1, x.toString)).toDF
new VariableFile (fileName, false, "", "UTF-8").writeHdfs(",")(df)
 val result = SparkContexts.context.read.csv(fileName).collect
result(0).getAs[String](0) mustBe "   あa  "
result(0).getAs[String](1) mustBe "1"
result(0).getAs[String](2) mustBe "a"
         }
"空値、null値のquoteなし確認" in {
         val fileName = s"${ inArgs.baseOutputFilePath }/variableFileHdfs/pt4"
 val df = List(Test2("", null, null, null, null)).toDF
new VariableFile (fileName, false, "", "UTF-8").writeHdfs(",")(df)
 val result = SparkContexts.context.read.text(fileName).collect
result(0).getAs[String](0) mustBe ",,,,"
         }
      }
"writeSequence" ignore {
      val fileName = s"${ inArgs.baseOutputFilePath }/variableFileSequence/pt1"
 val df = ('a' to 'c').map( x =>Test1(s"あ${ x }", 1, x.toString)).toDF
new VariableFile (fileName, false, "", "MS932", Seq("a", "b")).writeSequence(",")(df)
 val result = SparkContexts.sc.sequenceFile(fileName, NullWritable.get.getClass, Test1.getClass).map(_.toString).collect.sorted
result(0) mustBe "((null),82 a0 61 2c 31 2c 61)"
result(1) mustBe "((null),82 a0 62 2c 31 2c 62)"
result(2) mustBe "((null),82 a0 63 2c 31 2c 63)"
      }
"writeSequenceCsvWithDoubleQuote_MS932" ignore {
      val fileName = s"${ inArgs.baseOutputFilePath }/variableFileSequence/pt2"
 val df = ('a' to 'c').map( x =>Test1(s"あ${ x }", 1, x.toString)).toDF
new VariableFile (fileName, false, "", "MS932", Seq("a", "b")).writeSequenceCsvWithDoubleQuote(Set("c"))(df)
 val result = SparkContexts.sc.sequenceFile(fileName, NullWritable.get.getClass, Test1.getClass).map(_.toString).collect.sorted
result.foreach(println)
result(0) mustBe "((null),82 a0 61 2c 31 2c 22 61 22)"
result(1) mustBe "((null),82 a0 62 2c 31 2c 22 62 22)"
result(2) mustBe "((null),82 a0 63 2c 31 2c 22 63 22)"
      }
   }
}