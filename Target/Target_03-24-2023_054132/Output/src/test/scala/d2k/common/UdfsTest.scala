package d2k.common

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts.context
import context.implicits._
import spark.common.SparkContexts
import spark.common.SparkContexts
import com.snowflake.snowpark.functions._
import java.util.Date
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import com.snowflake.snowpark.DataFrame
import java.util.Calendar
import java.sql.Timestamp

/*EWI: SPRKSCL1142 => org.apache.spark.sql.expressions.UserDefinedFunction is not supported*/
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.text.SimpleDateFormat
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class Test (str: String)
case class DateTest (str: String, sqlDate: java.sql.Date, sqlTimestamp: java.sql.Timestamp)
case class DateTest2 (test1: DateTest, test2: DateTest)
case class DateRangeTestDateType (DT_BEGIN: java.sql.Date, DT_END: java.sql.Date, DT_DELETE: java.sql.Date)
case class DateRangeTestStringType (DT_BEGIN: String, DT_END: String, DT_DELETE: String)
case class DateDiffStr (target: String, target2: String = "")
case class DateDiffDate (target: java.sql.Date, target2: java.sql.Date)
case class DateDiffTimestamp (target: java.sql.Timestamp, target2: java.sql.Timestamp)
case class PostCodeNormalizeData (POSTCODE1: String, POSTCODE2: String = "")
 class UdfsTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "Kana Converter" should {
   "be call udf kana converter" in {
      val df = context.createDataFrame(Seq(Test("アイウエオ")))
 val result = df.withColumn("result", Udfs.kanaConv(df("str"))).collect
result(0).getAs[String]("result") mustBe "ｱｲｳｴｵ"
      }
"be call udf kana converter　for Search" in {
      val df = context.createDataFrame(Seq(Test("アイウエオ")))
 val result = df.withColumn("result", Udfs.kanaConvSearch(df("str"))).collect
result(0).getAs[String]("result") mustBe "ｱｲｳｴｵ"
      }
   }

   "DateCalc" should {
   "normal end" in {
      Udfs.dateCalc(DateTime.parse("2016-01-01T00:00:00"), 1, 0) mustBe DateTime.parse("2016-02-01T00:00:00")
Udfs.dateCalc(DateTime.parse("2016-01-01T00:00:00"), -1, 0) mustBe DateTime.parse("2015-12-01T00:00:00")
Udfs.dateCalc(DateTime.parse("2016-01-01T00:00:00"), 0, 1) mustBe DateTime.parse("2016-01-02T00:00:00")
Udfs.dateCalc(DateTime.parse("2016-01-01T00:00:00"), 0, -1) mustBe DateTime.parse("2015-12-31T00:00:00")
Udfs.dateCalc(DateTime.parse("2016-01-01T00:00:00"), 1, 1) mustBe DateTime.parse("2016-02-02T00:00:00")
Udfs.dateCalc(DateTime.parse("2016-01-01T00:00:00"), -1, -1) mustBe DateTime.parse("2015-11-30T00:00:00")
Udfs.dateCalc(DateTime.parse("2016-01-01T00:00:00"), 1, -1) mustBe DateTime.parse("2016-01-31T00:00:00")
Udfs.dateCalc(DateTime.parse("2016-01-01T00:00:00"), -1, 1) mustBe DateTime.parse("2015-12-02T00:00:00")
      }
"success null Check" in {
      Udfs.dateCalc(null, 1, 0) mustBe null
      }
   }

   "DateDuration" should {
   "success by Days" in {
      Udfs.dateDurationByDays(DateTime.parse("2016-01-01T00:00:00"), DateTime.parse("2016-01-01T00:00:00")) mustBe 0
Udfs.dateDurationByDays(DateTime.parse("2016-01-02T00:00:00"), DateTime.parse("2016-01-01T00:00:00")) mustBe -1
Udfs.dateDurationByDays(DateTime.parse("2015-12-31T00:00:00"), DateTime.parse("2016-01-01T00:00:00")) mustBe 1
Udfs.dateDurationByDays(DateTime.parse("2016-01-01T00:00:00"), DateTime.parse("2016-02-01T00:00:00")) mustBe 31
      }
"success by Months" in {
      Udfs.dateDurationByMonths(DateTime.parse("2016-01-01T00:00:00"), DateTime.parse("2016-01-01T00:00:00")) mustBe 0
Udfs.dateDurationByMonths(DateTime.parse("2016-01-01T00:00:00"), DateTime.parse("2016-02-01T00:00:00")) mustBe 1
Udfs.dateDurationByMonths(DateTime.parse("2016-01-01T00:00:00"), DateTime.parse("2015-12-01T00:00:00")) mustBe -1
Udfs.dateDurationByMonths(DateTime.parse("2016-01-01T00:00:00"), DateTime.parse("2017-01-01T00:00:00")) mustBe 12
      }
   }

   "udf calc" should {
   "success ym String calc" in {
      val patternYYYYMM = DateTimeFormat.forPattern("yyyyMMdd HHmmss")
import SparkContexts.context.implicits._
 val date = DateTime.parse("20160101 000000", patternYYYYMM)
 val sqlDate = new java.sql.Date (date.getMillis)
 val sqlTimestamp = new java.sql.Timestamp (date.getMillis)
 val df = SparkContexts.sc.makeRDD(Seq(DateTest("201601", null, null))).toDF
import Udfs.date.ym.string._
 val result = df.withColumn("result", calc(df("str"), lit(0))).collect
result(0).getAs[String]("result") mustBe "201601"
 val result2 = df.withColumn("result", calc(df("str"), lit(2))).collect
result2(0).getAs[String]("result") mustBe "201603"
 val result3 = df.withColumn("result", calc(df("str"), lit(-2))).collect
result3(0).getAs[String]("result") mustBe "201511"
      }
"success ym String duration" in {
      val patternYYYYMM = DateTimeFormat.forPattern("yyyyMMdd HHmmss")
import SparkContexts.context.implicits._
 val date = DateTime.parse("20160101 000000", patternYYYYMM)
 val sqlDate = new java.sql.Date (date.getMillis)
 val sqlTimestamp = new java.sql.Timestamp (date.getMillis)
 val df = SparkContexts.sc.makeRDD(
Seq(DateTest2(DateTest("201601", null, null), DateTest("201603", null, null)))).toDF
import Udfs.date.ym.string._
 val result = df.withColumn("result", duration(df("test1.str"), df("test2.str"))).collect
result(0).getAs[Integer]("result") mustBe 2
 val result2 = df.withColumn("result", duration(df("test2.str"), df("test1.str"))).collect
result2(0).getAs[Integer]("result") mustBe -2
      }
"success ymd String calc" in {
      val patternYYYYMM = DateTimeFormat.forPattern("yyyyMMdd HHmmss")
import SparkContexts.context.implicits._
 val date = DateTime.parse("20160101 000000", patternYYYYMM)
 val sqlDate = new java.sql.Date (date.getMillis)
 val sqlTimestamp = new java.sql.Timestamp (date.getMillis)
 val df = SparkContexts.sc.makeRDD(Seq(DateTest("20160101", null, null))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calc(df("str"), lit(0))).collect
result(0).getAs[String]("result") mustBe "20160101"
 val result2 = df.withColumn("result", calc(df("str"), lit(2))).collect
result2(0).getAs[String]("result") mustBe "20160103"
 val result3 = df.withColumn("result", calc(df("str"), lit(-2))).collect
result3(0).getAs[String]("result") mustBe "20151230"
      }
"success ymd String duration" in {
      val patternYYYYMM = DateTimeFormat.forPattern("yyyyMMdd HHmmss")
import SparkContexts.context.implicits._
 val date = DateTime.parse("20160101 000000", patternYYYYMM)
 val sqlDate = new java.sql.Date (date.getMillis)
 val sqlTimestamp = new java.sql.Timestamp (date.getMillis)
 val df = SparkContexts.sc.makeRDD(
Seq(DateTest2(DateTest("20160101", null, null), DateTest("20160103", null, null)))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", duration(df("test1.str"), df("test2.str"))).collect
result(0).getAs[Integer]("result") mustBe 2
 val result2 = df.withColumn("result", duration(df("test2.str"), df("test1.str"))).collect
result2(0).getAs[Integer]("result") mustBe -2
      }
   }

   "hashAuId" should {
   "input is null" in {
      execAssertEquals(null, null, Udfs.hashAuId)
      }
"input is zero length String" in {
      execAssertEquals("", "", Udfs.hashAuId)
      }
"input is all space" in {
      val input = "                             "
execAssertEquals(input, input, Udfs.hashAuId)
      }
   }

   "addPointToDateString" should {
   "normal 15Length" in {
      execAssertEquals("201601011122334", "20160101112233.4", Udfs.addPointToDateString)
      }
"normal 17Length" in {
      execAssertEquals("20160101112233456", "20160101112233.456", Udfs.addPointToDateString)
      }
"input is null" in {
      execAssertEquals(null, null, Udfs.addPointToDateString)
      }
"input is zero length String" in {
      execAssertEquals("", "", Udfs.addPointToDateString)
      }
   }

   "addHyphenToDateString" should {
   "normal date format" in {
      execAssertEquals("20160123", "2016-01-23", Udfs.addHyphenToDateString)
      }
"normal Timestamp format" in {
      execAssertEquals("20160123112233444", "2016-01-23", Udfs.addHyphenToDateString)
      }
"input is null" in {
      execAssertEquals(null, null, Udfs.addHyphenToDateString)
      }
"input is zero length String" in {
      execAssertEquals("", "", Udfs.addHyphenToDateString)
      }
   }

   "blankToZero" should {
   "normal " in {
      execAssertEquals("123", "123", Udfs.blankToZero)
      }
"input is null" in {
      execAssertEquals(null, "0", Udfs.blankToZero)
      }
"input is zero length String" in {
      execAssertEquals("", "0", Udfs.blankToZero)
      }
   }

   "trancateAfterPoint" should {
   "normal with fraction part" in {
      execAssertEquals("123.99", "123", Udfs.trancateAfterPoint)
      }
"normal with fraction part , sign" in {
      execAssertEquals("+123.00", "+123", Udfs.trancateAfterPoint)
      }
"normal with point , sign " in {
      execAssertEquals("-555.", "-555", Udfs.trancateAfterPoint)
      }
"normal without fraction part" in {
      execAssertEquals("999", "999", Udfs.trancateAfterPoint)
      }
"input is null" in {
      execAssertEquals(null, null, Udfs.trancateAfterPoint)
      }
"input is zero length String" in {
      execAssertEquals("", "", Udfs.trancateAfterPoint)
      }
   }

   //String引数1個のUDF検証用
   def execAssertEquals(input: String, expected: String, targetUdf: UserDefinedFunction) {
      val df = context.createDataFrame(Seq(Test(input)))
 val result = df.withColumn("result", targetUdf(df("str"))).collect
result(0).getAs[String]("result") mustBe expected
   }

   val FMT_YMD = DateTimeFormat.forPattern("yyyyMMdd")

   val MANG_DT_STR_TODAY = "20151001"

   val MANG_DT_STR_TOMORROW = "20151002"

   val MANG_DT_STR_INIT_HP = "0001-01-01"

   val MANG_DT_STR_INIT = "00010101"

   val MANG_DT_DATE_TODAY = new java.sql.Date (DateTime.parse(MANG_DT_STR_TODAY, FMT_YMD).getMillis)

   val MANG_DT_DATE_TOMORROW = new java.sql.Date (DateTime.parse(MANG_DT_STR_TOMORROW, FMT_YMD).getMillis)

   import Udfs.DateRangeStatus._

   "DateRangeStatus.getStatus" should {
   "normal string 0" in {
      execGetStatus(makeStringDf(MANG_DT_STR_TOMORROW, MANG_DT_STR_TOMORROW, null), STATUS_BEFORE)
      }
"normal string 9" in {
      execGetStatus(makeStringDf(MANG_DT_STR_TODAY, MANG_DT_STR_TODAY, null), STATUS_END)
      }
"normal string 1" in {
      execGetStatus(makeStringDf(MANG_DT_STR_TODAY, MANG_DT_STR_TOMORROW, null), STATUS_ON)
      }
"input is null string" in {
      execGetStatus(makeStringDf(null, null, null), STATUS_ON)
      }
"input is zero length string" in {
      execGetStatus(makeStringDf("", "", null), STATUS_END)
      }
"normal date 0" in {
      execGetStatus(makeDateDf(MANG_DT_DATE_TOMORROW, MANG_DT_DATE_TOMORROW, null), STATUS_BEFORE)
      }
"normal date 9" in {
      execGetStatus(makeDateDf(MANG_DT_DATE_TODAY, MANG_DT_DATE_TODAY, null), STATUS_END)
      }
"normal date 1" in {
      execGetStatus(makeDateDf(MANG_DT_DATE_TODAY, MANG_DT_DATE_TOMORROW, null), STATUS_ON)
      }
"input is null date" in {
      execGetStatus(makeDateDf(null, null, null), STATUS_ON)
      }
   }

   "DateRangeStatus.getStatusWithDeleteDate" should {
   "normal string 0" in {
      execGetStatusWithDeleteDate(makeStringDf(MANG_DT_STR_TOMORROW, MANG_DT_STR_TOMORROW, MANG_DT_STR_INIT), STATUS_BEFORE)
      }
"normal string 9" in {
      execGetStatusWithDeleteDate(makeStringDf(MANG_DT_STR_TODAY, MANG_DT_STR_TODAY, MANG_DT_STR_INIT_HP), STATUS_END)
      }
"normal string 1" in {
      execGetStatusWithDeleteDate(makeStringDf(MANG_DT_STR_TODAY, MANG_DT_STR_TOMORROW, ""), STATUS_ON)
      }
"delete string" in {
      execGetStatusWithDeleteDate(makeStringDf(MANG_DT_STR_TODAY, MANG_DT_STR_TOMORROW, MANG_DT_STR_TODAY), STATUS_END)
      }
"input is null string" in {
      execGetStatusWithDeleteDate(makeStringDf(null, null, null), STATUS_ON)
      }
"input is zero length string" in {
      execGetStatusWithDeleteDate(makeStringDf("", "", ""), STATUS_END)
      }
"normal date 0" in {
      execGetStatusWithDeleteDate(makeDateDf(MANG_DT_DATE_TOMORROW, MANG_DT_DATE_TOMORROW, null), STATUS_BEFORE)
      }
"normal date 9" in {
      execGetStatusWithDeleteDate(makeDateDf(MANG_DT_DATE_TODAY, MANG_DT_DATE_TODAY, null), STATUS_END)
      }
"normal date 1" in {
      execGetStatusWithDeleteDate(makeDateDf(MANG_DT_DATE_TODAY, MANG_DT_DATE_TOMORROW, null), STATUS_ON)
      }
"delete date" in {
      execGetStatusWithDeleteDate(makeDateDf(MANG_DT_DATE_TODAY, MANG_DT_DATE_TOMORROW, MANG_DT_DATE_TODAY), STATUS_END)
      }
"input is null date" in {
      execGetStatusWithDeleteDate(makeDateDf(null, null, null), STATUS_ON)
      }
   }

   "DateRangeStatus.getStatusWithBlankReplace" should {
   "normal string 0" in {
      execGetStatusWithBlankReplace(makeStringDf(MANG_DT_STR_TOMORROW, MANG_DT_STR_TOMORROW, null), STATUS_BEFORE)
      }
"normal string 9" in {
      execGetStatusWithBlankReplace(makeStringDf(MANG_DT_STR_TODAY, MANG_DT_STR_TODAY, null), STATUS_END)
      }
"normal string 1" in {
      execGetStatusWithBlankReplace(makeStringDf(MANG_DT_STR_TODAY, MANG_DT_STR_TOMORROW, null), STATUS_ON)
      }
"input is all null string" in {
      execGetStatusWithBlankReplace(makeStringDf(null, null, null), STATUS_BEFORE)
      }
"begin is null string" in {
      execGetStatusWithBlankReplace(makeStringDf(null, MANG_DT_STR_TODAY, null), STATUS_BEFORE)
      }
"end is all null string" in {
      execGetStatusWithBlankReplace(makeStringDf(MANG_DT_STR_TODAY, null, null), STATUS_ON)
      }
"input is all zero length string" in {
      execGetStatusWithBlankReplace(makeStringDf("", "", null), STATUS_BEFORE)
      }
"begin is zero length string" in {
      execGetStatusWithBlankReplace(makeStringDf("", MANG_DT_STR_TODAY, null), STATUS_BEFORE)
      }
"end is zero length string" in {
      execGetStatusWithBlankReplace(makeStringDf(MANG_DT_STR_TODAY, "", null), STATUS_ON)
      }
"normal date 0" in {
      execGetStatusWithBlankReplace(makeDateDf(MANG_DT_DATE_TOMORROW, MANG_DT_DATE_TOMORROW, null), STATUS_BEFORE)
      }
"normal date 9" in {
      execGetStatusWithBlankReplace(makeDateDf(MANG_DT_DATE_TODAY, MANG_DT_DATE_TODAY, null), STATUS_END)
      }
"normal date 1" in {
      execGetStatusWithBlankReplace(makeDateDf(MANG_DT_DATE_TODAY, MANG_DT_DATE_TOMORROW, null), STATUS_ON)
      }
"input is all null date" in {
      execGetStatusWithBlankReplace(makeDateDf(null, null, null), STATUS_BEFORE)
      }
"begin is null date" in {
      execGetStatusWithBlankReplace(makeDateDf(null, MANG_DT_DATE_TODAY, null), STATUS_BEFORE)
      }
"end is all null date" in {
      execGetStatusWithBlankReplace(makeDateDf(MANG_DT_DATE_TODAY, null, null), STATUS_ON)
      }
   }

   "DateRangeStatus.getStatusWithBlankReplaceAndDeleteDate" should {
   "normal string 0" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeStringDf(MANG_DT_STR_TOMORROW, MANG_DT_STR_TOMORROW, MANG_DT_STR_INIT), STATUS_BEFORE)
      }
"normal string 9" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeStringDf(MANG_DT_STR_TODAY, MANG_DT_STR_TODAY, MANG_DT_STR_INIT_HP), STATUS_END)
      }
"normal string 1" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeStringDf(MANG_DT_STR_TODAY, MANG_DT_STR_TOMORROW, ""), STATUS_ON)
      }
"delete string" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeStringDf(MANG_DT_STR_TODAY, MANG_DT_STR_TOMORROW, MANG_DT_STR_TODAY), STATUS_END)
      }
"input is all null string" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeStringDf(null, null, null), STATUS_ON)
      }
"begin is null string" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeStringDf(null, MANG_DT_STR_TODAY, null), STATUS_END)
      }
"end is all null string" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeStringDf(MANG_DT_STR_TODAY, null, null), STATUS_ON)
      }
"input is all zero length string" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeStringDf("", "", null), STATUS_ON)
      }
"begin is zero length string" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeStringDf("", MANG_DT_STR_TODAY, null), STATUS_END)
      }
"end is zero length string" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeStringDf(MANG_DT_STR_TODAY, "", null), STATUS_ON)
      }
"normal date 0" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeDateDf(MANG_DT_DATE_TOMORROW, MANG_DT_DATE_TOMORROW, null), STATUS_BEFORE)
      }
"normal date 9" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeDateDf(MANG_DT_DATE_TODAY, MANG_DT_DATE_TODAY, null), STATUS_END)
      }
"normal date 1" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeDateDf(MANG_DT_DATE_TODAY, MANG_DT_DATE_TOMORROW, null), STATUS_ON)
      }
"delete date" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeDateDf(MANG_DT_DATE_TODAY, MANG_DT_DATE_TOMORROW, MANG_DT_DATE_TODAY), STATUS_END)
      }
"input is all null date" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeDateDf(null, null, null), STATUS_ON)
      }
"begin is null date" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeDateDf(null, MANG_DT_DATE_TODAY, null), STATUS_END)
      }
"end is all null date" in {
      execGetStatusWithBlankReplaceAndDeleteDate(makeDateDf(MANG_DT_DATE_TODAY, null, null), STATUS_ON)
      }
   }

   // 日付ハサミコミ判定のみ
   def execGetStatus(df: DataFrame, expected: String) = assertEquals(df.withColumn("result", getStatus(df("DT_BEGIN"), df("DT_END"), lit(MANG_DT_STR_TODAY))), expected)

   // 削除年月日判定+日付ハサミコミ判定
   def execGetStatusWithDeleteDate(df: DataFrame, expected: String) = assertEquals(df.withColumn("result", getStatusWithDeleteDate(df("DT_BEGIN"), df("DT_END"), df("DT_DELETE"), lit(MANG_DT_STR_TODAY))), expected)

   // 初期値=>最大値 置換後、ハサミコミ判定
   def execGetStatusWithBlankReplace(df: DataFrame, expected: String) = assertEquals(df.withColumn("result", getStatusWithBlankReplace(df("DT_BEGIN"), df("DT_END"), lit(MANG_DT_STR_TODAY))), expected)

   // 初期値=>最大値 置換(endDateのみ)後、削除年月日判定+日付ハサミコミ判定
   def execGetStatusWithBlankReplaceAndDeleteDate(df: DataFrame, expected: String) = assertEquals(df.withColumn("result", getStatusWithBlankReplaceAndDeleteDate(df("DT_BEGIN"), df("DT_END"), df("DT_DELETE"), lit(MANG_DT_STR_TODAY))), expected)

   def assertEquals(df: DataFrame, expected: String) {
      val actual = df.collect()(0).getAs[String]("result")
actual mustBe expected
   }

   def makeStringDf(DT_BEGIN: String, DT_END: String, DT_DELETE: String) = context.createDataFrame(Seq(DateRangeTestStringType(DT_BEGIN, DT_END, DT_DELETE)))

   def makeDateDf(DT_BEGIN: java.sql.Date, DT_END: java.sql.Date, DT_DELETE: java.sql.Date) = context.createDataFrame(Seq(DateRangeTestDateType(DT_BEGIN, DT_END, DT_DELETE)))

   "udf calcAndDiff" should {
   "normal date" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("20160101"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("20160101", 0, 0)(df("target"))).collect
result(0).getAs[Boolean]("result") mustBe true
      }
"be success beginDay pattern" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("20160101"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("20160101", -1)(df("target"))).collect
result(0).getAs[Boolean]("result") mustBe true
      }
"be fail beginDay pattern false" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("20160101"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("20160101", 1)(df("target"))).collect
result(0).getAs[Boolean]("result") mustBe false
      }
"be success endDay pattern" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("20160101"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("20160101", 0, 1)(df("target"))).collect
result(0).getAs[Boolean]("result") mustBe true
      }
"be success endDay pattern false" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("20160101"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("20160101", 0, -1)(df("target"))).collect
result(0).getAs[Boolean]("result") mustBe false
      }
"be success beginDay pattern multi target" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("20160101", "20151230"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("20160101", -1)(df("target"), df("target2"))).collect
result(0).getAs[Boolean]("result") mustBe true
      }
"be success beginDay pattern multi target false" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("20151229", "20151230"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("20160101", -1)(df("target"), df("target2"))).collect
result(0).getAs[Boolean]("result") mustBe false
      }
"be success endDay pattern multi target" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("20160101", "20160102"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("20160101", -1, 1)(df("target"), df("target2"))).collect
result(0).getAs[Boolean]("result") mustBe true
      }
"be success endDay pattern multi target false" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("20151229", "20160103"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("20160101", -1, 1)(df("target"), df("target2"))).collect
result(0).getAs[Boolean]("result") mustBe false
      }
"be true by null value in targets" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr(null))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("20160101", 0, 0)(df("target"))).collect
result(0).getAs[Boolean]("result") mustBe true
      }
"be true by empty value in targets" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr(""))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("20160101", 0, 0)(df("target"))).collect
result(0).getAs[Boolean]("result") mustBe true
      }
"be true by illegal format in targets" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("xxxx"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("20160101", 0, 0)(df("target"))).collect
result(0).getAs[Boolean]("result") mustBe true
      }
"be true by null value in base" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("20160101"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff(null, 0, 0)(df("target"))).collect
result(0).getAs[Boolean]("result") mustBe true
      }
"be true by empty value in base" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("20160101"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("", 0, 0)(df("target"))).collect
result(0).getAs[Boolean]("result") mustBe true
      }
"be true by illegal format in base" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("20160101"))).toDF
import Udfs.date.ymd.string._
 val result = df.withColumn("result", calcAndDiff("xxx", 0, 0)(df("target"))).collect
result(0).getAs[Boolean]("result") mustBe true
      }
   }

   "dateCnvJpToEu" should {
   "normal" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("H", "281019"))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "20161019"
      }
"normal2" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("", "281019"))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "00010101"
      }
"normal3" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("H", ""))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "00010101"
      }
"normal4" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("", ""))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "00010101"
      }
"normal5" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("S", "131019"))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "19381019"
      }
"normal6" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("T", "131019"))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "19241019"
      }
"normal7" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("M", "131019"))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "18801019"
      }
"normal8" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("3", "281019"))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "20161019"
      }
"normal9" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("2", "131019"))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "19381019"
      }
"normal10" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("1", "131019"))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "19241019"
      }
"normal11" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("0", "131019"))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "18801019"
      }
"normal12" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("3", ""))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "00010101"
      }
"new era" when {
      "normal input new era number" in {
         val df = SparkContexts.sc.makeRDD(
Seq(
DateDiffStr("4", "010101"), 
DateDiffStr("4", "010430"), 
DateDiffStr("4", "010501"), 
DateDiffStr("4", "991231"), 
DateDiffStr("4", ""))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "20190101"
result(1).getAs[String]("result") mustBe "20190430"
result(2).getAs[String]("result") mustBe "20190501"
result(3).getAs[String]("result") mustBe "21171231"
result(4).getAs[String]("result") mustBe "00010101"
         }
"normal input new era character" in {
         val df = SparkContexts.sc.makeRDD(
Seq(
DateDiffStr("K", "010101"), 
DateDiffStr("K", "010430"), 
DateDiffStr("K", "010501"), 
DateDiffStr("K", "991231"), 
DateDiffStr("K", ""))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEu(df("target"), df("target2"))).collect
result(0).getAs[String]("result") mustBe "20190101"
result(1).getAs[String]("result") mustBe "20190430"
result(2).getAs[String]("result") mustBe "20190501"
result(3).getAs[String]("result") mustBe "21171231"
result(4).getAs[String]("result") mustBe "00010101"
         }
      }
   }

   "dateCnvJpToEuWithDefault" should {
   "normal" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr("H", "281019"))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEuWithDefault(df("target"), df("target2"), lit("29991231"))).collect
result(0).getAs[String]("result") mustBe "20161019"
      }
"abnormal" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr(" ", "281019"))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEuWithDefault(df("target"), df("target2"), lit("29991231"))).collect
result(0).getAs[String]("result") mustBe "29991231"
      }
"abnormal1" in {
      val df = SparkContexts.sc.makeRDD(
Seq(DateDiffStr(" ", "281019"))).toDF
 val result = df.withColumn("result", Udfs.dateCnvJpToEuWithDefault(df("target"), df("target2"), lit("00010101"))).collect
result(0).getAs[String]("result") mustBe "00010101"
      }
   }

   "udf isNaOrNull" should {
   "be normal end" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(Test(""), Test(null))).toDF
 val result = df.filter(Udfs.isNaOrNull(col("str"))).collect
result(0).getAs[String]("str") mustBe null
result.size mustBe 1
      }
   }

   "udf isNotNaAndNotNull" should {
   "be normal end" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(Test(""), Test(null))).toDF
 val result = df.filter(Udfs.isNotNaAndNotNull(col("str"))).collect
result(0).getAs[String]("str") mustBe ""
result.size mustBe 1
      }
   }

   def testCutLimitStr(input: String, cutLen: Int, exp: String) = {
   import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(Test(input))).toDF
 val result = df.withColumn("result", Udfs.cutLimitStr(col("str"), lit(cutLen))).collect
result(0).getAs[String]("result") mustBe exp
   }

   "udf cutLimitStr" should {
   "success not half" in {
      testCutLimitStr("ほげ", 2, "ほ")
      }
"success half" in {
      testCutLimitStr("ほげ", 3, "ほ")
      }
"success full" in {
      testCutLimitStr("ほげ", 4, "ほげ")
      }
"success over" in {
      testCutLimitStr("ほげ", 5, "ほげ")
      }
"success null" in {
      testCutLimitStr(null, 1, null)
      }
"success empty" in {
      testCutLimitStr("", 1, "")
      }
   }

   def testCalcSchoolAge(birthY: Int, birthM: Int, birthD: Int, runningYMD: String, exp: Int) = {
   import SparkContexts.context.implicits._
 val cal = Calendar.getInstance
cal.set(Calendar.YEAR, birthY)
cal.set(Calendar.MONTH, birthM)
cal.set(Calendar.DATE, birthD)
 val df = SparkContexts.sc.makeRDD(
Seq(DateTest(runningYMD, null, new java.sql.Timestamp (cal.getTime.getTime)))).toDF
 val result = df.withColumn("result", Udfs.calcSchoolAge(df("sqlTimestamp"), df("str"))).collect
result(0).getAs[Integer]("result") mustBe exp
   }

   "udf calcSchoolAge" should {
   "success birth 0402 run 0402" in {
      testCalcSchoolAge(2000, 3, 2, "20170402", 17)
      }
"success birth 1231 run 1231" in {
      testCalcSchoolAge(2000, 11, 31, "20101231", 10)
      }
"success birth 0101 run 0402" in {
      testCalcSchoolAge(2000, 0, 1, "20170402", 18)
      }
"success birth 0401 run 1231" in {
      testCalcSchoolAge(2000, 3, 1, "20101231", 11)
      }
"success birth 0402 run 0101" in {
      testCalcSchoolAge(2000, 3, 2, "20090101", 8)
      }
"success birth 1231 run 0401" in {
      testCalcSchoolAge(2000, 11, 31, "20090401", 8)
      }
"success birth 0101 run 0101" in {
      testCalcSchoolAge(2000, 0, 1, "20090101", 9)
      }
"success  birth 0401 run 0401" in {
      testCalcSchoolAge(2000, 3, 1, "20090401", 9)
      }
"success MAX EQUAL" in {
      testCalcSchoolAge(2000, 3, 2, "29990402", 999)
      }
"success MAX OVER" in {
      testCalcSchoolAge(2000, 3, 2, "30000402", 999)
      }
"illegal output" when {
      "schoolAge is under 0" in {
         testCalcSchoolAge(2017, 4, 2, "20160402", 999)
         }
      }
"success input null" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateTest("20150101", null, null))).toDF
 val result = df.withColumn("result", Udfs.calcSchoolAge(df("sqlTimestamp"), df("str"))).collect
result(0).getAs[Integer]("result") mustBe 999
      }
   }

   def testCalcAge(birthYMD: String, runningYMD: String, exp: Int) = {
   var simpleDateFormat = new SimpleDateFormat ("yyyyMMdd")
 var date = simpleDateFormat.parse(birthYMD)
 val df = SparkContexts.sc.makeRDD(
Seq(DateTest(runningYMD, null, new java.sql.Timestamp (date.getTime)))).toDF
 val result = df.withColumn("result", Udfs.calcAge(df("sqlTimestamp"), df("str"))).collect
result(0).getAs[Integer]("result") mustBe exp
   }

   "udf calcAge" should {
   "success birth 0401 run 0401" in {
      testCalcAge("20000401", "20170401", 17)
      }
"success birth 0401 run 0501" in {
      testCalcAge("20000401", "20170501", 17)
      }
"success birth 0501 run 0401" in {
      testCalcAge("20000501", "20170401", 16)
      }
"success MAX EQUAL" in {
      testCalcAge("20000401", "29990501", 999)
      }
"success MAX OVER" in {
      testCalcAge("20000401", "30000501", 999)
      }
"illegal output" when {
      "calcAge reslt is under 0" in {
         testCalcAge("20000401", "19990401", 999)
         }
      }
"success input null" in {
      import SparkContexts.context.implicits._
 val df = SparkContexts.sc.makeRDD(
Seq(DateTest("20150101", null, null))).toDF
 val result = df.withColumn("result", Udfs.calcAge(df("sqlTimestamp"), df("str"))).collect
result(0).getAs[Integer]("result") mustBe 999
      }
   }

   "domainConvert" should {
   "call Domain Converter" in {
      val df = Seq(Test("x")).toDF
 val result = df.withColumn("str", Udfs.domainConvert(col("str"), lit("年月日"))).collect
result(0).getAs[String]("str") mustBe "00010101"
      }
   }

   "MakeDate date_yyyyMMdd" should {
   "be success　convert" in {
      val df = Seq(Test("20170102")).toDF
 val result = df.withColumn("str", Udfs.MakeDate.date_yyyyMMdd(col("str"))).collect
result(0).getAs[Date]("str").toString mustBe "2017-01-02"
      }
   }

   "MakeDate timestamp_yyyyMMdd" should {
   "be success　convert" in {
      val df = Seq(Test("20170102")).toDF
 val result = df.withColumn("str", Udfs.MakeDate.timestamp_yyyyMMdd(col("str"))).collect
result(0).getAs[Timestamp]("str").toString mustBe "2017-01-02 00:00:00.0"
      }
   }

   "MakeDate timestamp_yyyyMMddhhmmss" should {
   "be success　convert" in {
      val df = Seq(Test("20170102030405")).toDF
 val result = df.withColumn("str", Udfs.MakeDate.timestamp_yyyyMMddhhmmss(col("str"))).collect
result(0).getAs[Timestamp]("str").toString mustBe "2017-01-02 03:04:05.0"
      }
   }

   "MakeDate timestamp_yyyyMMddhhmmssSSS" should {
   "be success　convert" in {
      val df = Seq(Test("20170102030405006")).toDF
 val result = df.withColumn("str", Udfs.MakeDate.timestamp_yyyyMMddhhmmssSSS(col("str"))).collect
result(0).getAs[Timestamp]("str").toString mustBe "2017-01-02 03:04:05.006"
      }
   }

   "PostCodeNormalizer" should {
   import d2k.common.Udfs.PostCodeNormalizer._
"normal end" when {
      "single" in {
         val df = Seq(PostCodeNormalizeData("300-1")).toDF
 val result = df.withColumn("RESULT", single($"POSTCODE1")).collect
result(0).getAs[String]("RESULT") mustBe "300"
         }
"parent" in {
         val df = Seq(PostCodeNormalizeData("400")).toDF
 val result = df.withColumn("RESULT", parent($"POSTCODE1")).collect
result(0).getAs[String]("RESULT") mustBe "400"
         }
"child" in {
         val df = Seq(PostCodeNormalizeData("55")).toDF
 val result = df.withColumn("RESULT", child($"POSTCODE1")).collect
result(0).getAs[String]("RESULT") mustBe "55"
         }
      }
   }
}