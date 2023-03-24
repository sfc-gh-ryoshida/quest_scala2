package spark.common

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts.context
import context.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import scala.util.Try
import org.joda.time.DateTime
import java.sql.Timestamp
import java.sql.Date
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import java.util.Properties

class DbCtlTest extends WordSpec with MustMatchers with BeforeAndAfter {
  import org.apache.spark.sql.types._
  val currentTime = System.currentTimeMillis
  def d2s(dateMill: Long) = new DateTime(dateMill).toString("yyyy-MM-dd")
  def date2s(date: Date) = new DateTime(date).toString("yyyy-MM-dd")
  def d2s(date: Timestamp) = new DateTime(date).toString("yyyy-MM-dd")

  val dbtarget = DbCtl.dbInfo1

  val structTypeAnyType = StructType(Seq(
    StructField("KEY1", StringType), StructField("KEY2", DateType), StructField("KEY3", TimestampType),
    StructField("KEY4", IntegerType), StructField("KEY5", LongType),
    StructField("KEY6", DecimalType(5, 1)), StructField("VALUE", StringType)))

  val structType5 = StructType(Seq(
    StructField("TEST", StringType), StructField("TEST2", StringType), StructField("TEST3", StringType),
    StructField("TEST4", StringType), StructField("TEST5", StringType), StructField("DDATE", DateType), StructField("DTIMESTAMP", TimestampType)))
  val tableName5 = "deleteTest5"

  "DbCtl.insertAccelerated" should {
    val tableName = "insertNotExist3"
    "insert recs" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA1", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff1"),
        Row("AAA2", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff2"),
        Row("AAA3", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff3"))), structTypeAnyType)

      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      val result = dbCtl.readTable(tableName).sort("KEY1").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("KEY1") mustBe "AAA1"
      result(0).getAs[String]("VALUE") mustBe "fff1"
      result(1).getAs[String]("KEY1") mustBe "AAA2"
      result(1).getAs[String]("VALUE") mustBe "fff2"
      result(2).getAs[String]("KEY1") mustBe "AAA3"
      result(2).getAs[String]("VALUE") mustBe "fff3"
    }

    "insert recs with commit size" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA1", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff1"),
        Row("AAA2", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff2"),
        Row("AAA3", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff3"))), structTypeAnyType)

      val dbCtl = new DbCtl(dbtarget.copy(commitSize = Some(2)))
      dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      val result = dbCtl.readTable(tableName).sort("KEY1").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("KEY1") mustBe "AAA1"
      result(0).getAs[String]("VALUE") mustBe "fff1"
      result(1).getAs[String]("KEY1") mustBe "AAA2"
      result(1).getAs[String]("VALUE") mustBe "fff2"
      result(2).getAs[String]("KEY1") mustBe "AAA3"
      result(2).getAs[String]("VALUE") mustBe "fff3"
    }

    "be normal end. insert recs null pattern test." in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", null, null, null, null, null, null))), structTypeAnyType)

      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertAccelerated(df, "insertAcc", SaveMode.Overwrite)
      val result = dbCtl.readTable("insertAcc").sort("KEY1").collect.toList
      result.length mustBe 1
      result(0).getAs[String]("KEY1") mustBe "KEY1"
      result(0).getAs[Date]("KEY2") mustBe null
      result(0).getAs[Timestamp]("KEY3") mustBe null
      result(0).getAs[Integer]("KEY4") mustBe null
      result(0).getAs[Number]("KEY5") mustBe null
      result(0).getAs[Decimal]("KEY6") mustBe null
      result(0).getAs[String]("VALUE") mustBe null
    }

    "be normal end. insert recs null pattern no type test." in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", null, null, null, null, null, null))), structTypeAnyType)
        .drop("KEY2").drop("KEY3").drop("KEY4").drop("KEY5").drop("KEY6").drop("VALUE")
        .withColumn("KEY2", lit(null))
        .withColumn("KEY3", lit(null))
        .withColumn("KEY4", lit(null))
        .withColumn("KEY5", lit(null))
        .withColumn("KEY6", lit(null))
        .withColumn("VALUE", lit(null))

      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertAccelerated(df, "insertAcc", SaveMode.Overwrite)
      val result = dbCtl.readTable("insertAcc").sort("KEY1").collect.toList
      result.length mustBe 1
      result(0).getAs[Date]("KEY2") mustBe null
      result(0).getAs[Timestamp]("KEY3") mustBe null
      result(0).getAs[Integer]("KEY4") mustBe null
      result(0).getAs[Number]("KEY5") mustBe null
      result(0).getAs[Decimal]("KEY6") mustBe null
      result(0).getAs[String]("VALUE") mustBe null
    }
  }

  "DbCtl.insertAccelerated with Direct Path Insert Mode" should {
    val tableName = "directInsert"
    "insert recs" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA1", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff1"),
        Row("AAA2", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff2"),
        Row("AAA3", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff3"))), structTypeAnyType)

      val dbCtl = new DbCtl(dbtarget.copy(isDirectPathInsertMode = true))
      dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      val result = dbCtl.readTable(tableName).sort("KEY1").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("KEY1") mustBe "AAA1"
      result(0).getAs[String]("VALUE") mustBe "fff1"
      result(1).getAs[String]("KEY1") mustBe "AAA2"
      result(1).getAs[String]("VALUE") mustBe "fff2"
      result(2).getAs[String]("KEY1") mustBe "AAA3"
      result(2).getAs[String]("VALUE") mustBe "fff3"
    }

    "insert recs with commit size" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA1", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff1"),
        Row("AAA2", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff2"),
        Row("AAA3", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff3"))), structTypeAnyType)

      val dbCtl = new DbCtl(dbtarget.copy(commitSize = Some(2), isDirectPathInsertMode = true))
      dbCtl.insertAccelerated(df, tableName, SaveMode.Overwrite)
      val result = dbCtl.readTable(tableName).sort("KEY1").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("KEY1") mustBe "AAA1"
      result(0).getAs[String]("VALUE") mustBe "fff1"
      result(1).getAs[String]("KEY1") mustBe "AAA2"
      result(1).getAs[String]("VALUE") mustBe "fff2"
      result(2).getAs[String]("KEY1") mustBe "AAA3"
      result(2).getAs[String]("VALUE") mustBe "fff3"
    }
  }

  "DbCtl.insertNotExist" should {
    val tableName = "insertNotExist1"
    val tableName2 = "insertNotExist2"
    val tableName3 = "insertNotExist3"
    val tableName4 = "insertNotExist4"
    val structType = StructType(Seq(
      StructField("KEY1", StringType), StructField("KEY2", StringType), StructField("KEY3", StringType),
      StructField("KEY4", StringType), StructField("KEY5", StringType), StructField("VALUE", StringType)))
    val structTypeAnyType = StructType(Seq(
      StructField("KEY1", StringType), StructField("KEY2", DateType), StructField("KEY3", TimestampType),
      StructField("KEY4", IntegerType), StructField("KEY5", LongType),
      StructField("KEY6", DecimalType(5, 1)), StructField("VALUE", StringType)))

    "insert recs" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA1", "BBB", "CCC", "ddd", "eee", "fff1"),
        Row("AAA2", "BBB", "CCC", "ddd", "eee", "fff2"),
        Row("AAA3", "BBB", "CCC", "ddd", "eee", "fff3"),
        Row("AAA1", "BBB", "CCC", "ddd", "eee", "fff4"),
        Row("AAA2", "BBB", "CCC", "ddd", "eee", "fff5"),
        Row("AAA3", "BBB", "CCC", "ddd", "eee", "fff6"))), structType)

      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertNotExists(df.repartition(1), tableName, Seq("KEY1", "KEY2", "KEY3", "KEY4", "KEY5"), SaveMode.Overwrite)
      val result = dbCtl.readTable(tableName).sort("KEY1").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("KEY1") mustBe "AAA1"
      result(0).getAs[String]("VALUE") mustBe "fff1"
      result(1).getAs[String]("KEY1") mustBe "AAA2"
      result(1).getAs[String]("VALUE") mustBe "fff2"
      result(2).getAs[String]("KEY1") mustBe "AAA3"
      result(2).getAs[String]("VALUE") mustBe "fff3"
    }

    "insert recs. normal end pattern1" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA1", "BBB", "CCC", "ddd", "eee", "fff1"),
        Row("AAA2", "BBB", "CCC", "ddd", "eee", "fff2"),
        Row("AAA3", "BBB", "CCC", "ddd", "eee", "fff3"),
        Row("AAA1", "BBB", "CCC", "ddd", "eee", "fff4"),
        Row("AAA2", "BBB", "CCC", "ddd", "eee", "fff5"),
        Row("AAA3", "BBB", "CCC", "ddd", "eee", "fff6"))), structType)

      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertNotExists(df.repartition(1), tableName, Seq("KEY1"), SaveMode.Overwrite)
      val result = dbCtl.readTable(tableName).sort("KEY1").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("KEY1") mustBe "AAA1"
      result(0).getAs[String]("VALUE") mustBe "fff1"
      result(1).getAs[String]("KEY1") mustBe "AAA2"
      result(1).getAs[String]("VALUE") mustBe "fff2"
      result(2).getAs[String]("KEY1") mustBe "AAA3"
      result(2).getAs[String]("VALUE") mustBe "fff3"
    }

    "insert recs. normal end pattern2" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA1", "BBB", "CCC", "ddd", "eee", "fff1"),
        Row("AAA2", "BBB", "CCC", "ddd", "eee", "fff2"),
        Row("AAA3", "BBB", "CCC", "ddd", "eee", "fff3"),
        Row("AAA1", "BBB", "CCC", "ddd", "eee", "fff4"),
        Row("AAA2", "BBB", "CCC", "ddd", "eee", "fff5"),
        Row("AAA3", "BBB", "CCC", "ddd", "eee", "fff6"))), structType)

      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertNotExists(df.repartition(1), tableName, Seq("KEY1", "KEY2"), SaveMode.Overwrite)
      val result = dbCtl.readTable(tableName).sort("KEY1").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("KEY1") mustBe "AAA1"
      result(0).getAs[String]("VALUE") mustBe "fff1"
      result(1).getAs[String]("KEY1") mustBe "AAA2"
      result(1).getAs[String]("VALUE") mustBe "fff2"
      result(2).getAs[String]("KEY1") mustBe "AAA3"
      result(2).getAs[String]("VALUE") mustBe "fff3"
    }

    "insert recs. illegal pattern1" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA1", "BBB", "CCC", "ddd", "eee", "fff1"),
        Row("AAA2", "BBB", "CCC", "ddd", "eee", "fff2"),
        Row("AAA3", "BBB", "CCC", "ddd", "eee", "fff3"),
        Row("AAA1", "BBB", "CCC", "ddd", "eee", "fff4"),
        Row("AAA2", "BBB", "CCC", "ddd", "eee", "fff5"),
        Row("AAA3", "BBB", "CCC", "ddd", "eee", "fff6"))), structType)

      val dbCtl = new DbCtl(dbtarget)

      try {
        dbCtl.insertNotExists(df.repartition(1), tableName, Seq("KEY1", "KEY3"), SaveMode.Overwrite)
        fail
      } catch {
        case t: Throwable => t.printStackTrace()
      }
    }

    "insert recs. illegal pattern2" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA1", "BBB", "CCC", "ddd", "eee", "fff1"),
        Row("AAA2", "BBB", "CCC", "ddd", "eee", "fff2"),
        Row("AAA3", "BBB", "CCC", "ddd", "eee", "fff3"),
        Row("AAA1", "BBB", "CCC", "ddd", "eee", "fff4"),
        Row("AAA2", "BBB", "CCC", "ddd", "eee", "fff5"),
        Row("AAA3", "BBB", "CCC", "ddd", "eee", "fff6"))), structType)

      val dbCtl = new DbCtl(dbtarget)

      try {
        dbCtl.insertNotExists(df.repartition(1), tableName, Seq("KEY2"), SaveMode.Overwrite)
        fail
      } catch {
        case t: Throwable => t.printStackTrace()
      }
    }

    "insert recs.　reverse　keys sequence test" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA1", "BBB", "CCC", "ddd", "eee", "fff1"),
        Row("AAA2", "BBB", "CCC", "ddd", "eee", "fff2"),
        Row("AAA3", "BBB", "CCC", "ddd", "eee", "fff3"),
        Row("AAA1", "BBB", "CCC", "ddd", "eee", "fff4"),
        Row("AAA2", "BBB", "CCC", "ddd", "eee", "fff5"),
        Row("AAA3", "BBB", "CCC", "ddd", "eee", "fff6"))), structType)

      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertNotExists(df.repartition(1), tableName2, Seq("KEY5", "KEY4", "KEY3", "KEY2", "KEY1"), SaveMode.Overwrite)
      val result = dbCtl.readTable(tableName2).sort("KEY1").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("KEY1") mustBe "AAA1"
      result(0).getAs[String]("VALUE") mustBe "fff1"
      result(1).getAs[String]("KEY1") mustBe "AAA2"
      result(1).getAs[String]("VALUE") mustBe "fff2"
      result(2).getAs[String]("KEY1") mustBe "AAA3"
      result(2).getAs[String]("VALUE") mustBe "fff3"
    }

    "insert recs.　any type test." in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA1", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff1"),
        Row("AAA2", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff2"),
        Row("AAA3", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff3"),
        Row("AAA1", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff4"),
        Row("AAA2", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff5"),
        Row("AAA3", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff6"))), structTypeAnyType)

      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertNotExists(df.repartition(1), tableName3, Seq("KEY1", "KEY2", "KEY3", "KEY4", "KEY5", "KEY6"), SaveMode.Overwrite)
      val result = dbCtl.readTable(tableName3).sort("KEY1").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("KEY1") mustBe "AAA1"
      result(0).getAs[String]("VALUE") mustBe "fff1"
      result(1).getAs[String]("KEY1") mustBe "AAA2"
      result(1).getAs[String]("VALUE") mustBe "fff2"
      result(2).getAs[String]("KEY1") mustBe "AAA3"
      result(2).getAs[String]("VALUE") mustBe "fff3"
    }

    "insert recs.　append test." in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA1", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff1"),
        Row("AAA2", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff2"),
        Row("AAA3", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff3"),
        Row("AAA1", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff4"),
        Row("AAA2", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff5"),
        Row("AAA3", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff6"))), structTypeAnyType)

      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertNotExists(df.repartition(1), tableName3, Seq("KEY1", "KEY2", "KEY3", "KEY4", "KEY5", "KEY6"), SaveMode.Overwrite)

      val df2 = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA4", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff7"),
        Row("AAA5", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff8"),
        Row("AAA6", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fff9"),
        Row("AAA4", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fffA"),
        Row("AAA5", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fffB"),
        Row("AAA6", new Date(currentTime), new Timestamp(currentTime), 10, 100L, BigDecimal(100.1), "fffC"))), structTypeAnyType)

      dbCtl.insertNotExists(df2.repartition(1), tableName3, Seq("KEY1", "KEY2", "KEY3", "KEY4", "KEY5", "KEY6"), SaveMode.Append)
      val result = dbCtl.readTable(tableName3).sort("KEY1").collect.toList
      result.length mustBe 6
      result(0).getAs[String]("KEY1") mustBe "AAA1"
      result(0).getAs[String]("VALUE") mustBe "fff1"
      result(1).getAs[String]("KEY1") mustBe "AAA2"
      result(1).getAs[String]("VALUE") mustBe "fff2"
      result(2).getAs[String]("KEY1") mustBe "AAA3"
      result(2).getAs[String]("VALUE") mustBe "fff3"

      result(3).getAs[String]("KEY1") mustBe "AAA4"
      result(3).getAs[String]("VALUE") mustBe "fff7"
      result(4).getAs[String]("KEY1") mustBe "AAA5"
      result(4).getAs[String]("VALUE") mustBe "fff8"
      result(5).getAs[String]("KEY1") mustBe "AAA6"
      result(5).getAs[String]("VALUE") mustBe "fff9"
    }

    "be normal end. insert recs null pattern test." in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", null, null, null, null, null, null))), structTypeAnyType)
      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertAccelerated(df, tableName4, SaveMode.Overwrite)
      val df2 = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY2", null, null, null, null, null, null))), structTypeAnyType)

      dbCtl.insertNotExists(df2.repartition(1), tableName4, Seq("KEY1"), SaveMode.Append)
      val result = dbCtl.readTable(tableName4).sort("KEY1").collect.toList
      result.length mustBe 2
      result(1).getAs[String]("KEY1") mustBe "KEY2"
      result(1).getAs[Date]("KEY2") mustBe null
      result(1).getAs[Timestamp]("KEY3") mustBe null
      result(1).getAs[Integer]("KEY4") mustBe null
      result(1).getAs[Number]("KEY5") mustBe null
      result(1).getAs[Decimal]("KEY6") mustBe null
      result(1).getAs[String]("VALUE") mustBe null
    }
  }

  "DbCtl.deleteRecords" should {
    val tableName = "DELETETEST"
    val structType = StructType(Seq(
      StructField("TEST", StringType), StructField("DDATE", DateType), StructField("DTIMESTAMP", TimestampType)))
    "delete one rec" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("AAA", new Date(new DateTime().getMillis), new Timestamp(new DateTime().getMillis)),
          Row("BBB", new Date(new DateTime().getMillis), new Timestamp(new DateTime().getMillis)),
          Row("CCC", new Date(new DateTime().getMillis), new Timestamp(new DateTime().getMillis)))), structType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("BBB", new Date(new DateTime().getMillis), new Timestamp(new DateTime().getMillis)))), structType)
      dbCtl.deleteRecords(delDf, tableName, Set("TEST"))

      val result = dbCtl.readTable(tableName).sort("TEST").collect.toList
      result.length mustBe 2
      result(0).getAs[String]("TEST") mustBe "AAA"
      result(1).getAs[String]("TEST") mustBe "CCC"
    }

    "delete one rec. over 5 columns" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("AAA", "AAA2", "AAA3", "AAA4", "AAA5", new Date(new DateTime().getMillis), new Timestamp(new DateTime().getMillis)),
          Row("BBB", "BBB2", "BBB3", "BBB4", "BBB5", new Date(new DateTime().getMillis), new Timestamp(new DateTime().getMillis)),
          Row("CCC", "CCC2", "CCC3", "CCC4", "CCC5", new Date(new DateTime().getMillis), new Timestamp(new DateTime().getMillis)))), structType5)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.autoCreateTable(tableName5)
        df.writeTable(tableName5, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("BBB", "BBB2", "BBB3", "BBB4", "BBB5",
          new Date(new DateTime().getMillis), new Timestamp(new DateTime().getMillis)))), structType5)
      dbCtl.deleteRecords(delDf, tableName5, Set("TEST", "TEST2", "TEST3", "TEST4", "TEST5"))

      val result = dbCtl.readTable(tableName5).sort("TEST").collect.toList
      result.length mustBe 2
      result(0).getAs[String]("TEST") mustBe "AAA"
      result(1).getAs[String]("TEST") mustBe "CCC"
    }

    "delete all recs" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("AAA", new Date(new DateTime().getMillis), new Timestamp(new DateTime().getMillis)),
          Row("BBB", new Date(new DateTime().getMillis), new Timestamp(new DateTime().getMillis)),
          Row("CCC", new Date(new DateTime().getMillis), new Timestamp(new DateTime().getMillis)))), structType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName, SaveMode.Overwrite)
        dbCtl.readTable(tableName).show
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("AAA"), Row("BBB"), Row("CCC"))), StructType(Seq(StructField("TEST", StringType))))
      dbCtl.deleteRecords(delDf, tableName, Set("TEST"))

      val result = dbCtl.readTable(tableName).collect
      result.length mustBe 0
    }

    "delete Key by date" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("AAA", new Date(currentTime - 10000000000L), new Timestamp(currentTime - 1000)),
          Row("BBB", new Date(currentTime), new Timestamp(currentTime - 1000)),
          Row("CCC", new Date(currentTime - 10000000000L), new Timestamp(currentTime - 1000)))), structType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("BBB", new Date(currentTime), new Timestamp(currentTime)))), structType)
      dbCtl.deleteRecords(delDf, tableName, Set("DDATE"))

      val result = dbCtl.readTable(tableName).collect.toList.sortBy(_.getAs[String]("TEST"))
      result.length mustBe 2
      result(0).getAs[String]("TEST") mustBe "AAA"
      result(1).getAs[String]("TEST") mustBe "CCC"
    }

    "delete Key by timestamp" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("AAA", new Date(currentTime - 10000000000L), new Timestamp(currentTime - 1000)),
          Row("BBB", new Date(currentTime - 10000000000L), new Timestamp(currentTime)),
          Row("CCC", new Date(currentTime - 10000000000L), new Timestamp(currentTime - 1000)))), structType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.repartition(1).writeTable(tableName, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("BBB", new Date(currentTime), new Timestamp(currentTime)))), structType)
      dbCtl.deleteRecords(delDf.repartition(1), tableName, Set("DTIMESTAMP"))

      val result = dbCtl.readTable(tableName).collect.toList.sortBy(_.getAs[String]("TEST"))
      result.length mustBe 2
      result(0).getAs[String]("TEST") mustBe "AAA"
      result(1).getAs[String]("TEST") mustBe "CCC"
    }
  }

  "DbCtl.updateRecords" should {
    val tableName = "updatetest"
    val tableName2 = "updatetest2"
    val stringStructType = StructType(Seq(
      StructField("KEY", StringType),
      StructField("V1", StringType),
      StructField("V2", StringType)))

    val anyStructType = StructType(Seq(
      StructField("KEY", StringType),
      StructField("DDATE", DateType),
      StructField("DTIMESTAMP", TimestampType)))

    val stringStructType7Columns = StructType(Seq(
      StructField("KEY", StringType),
      StructField("V1", StringType),
      StructField("V2", StringType),
      StructField("V3", StringType),
      StructField("V4", StringType),
      StructField("V5", StringType),
      StructField("V6", StringType)))

    val datetimeFormatter = DateTimeFormat.forPattern("yyyyMMdd")
    "update one rec" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", new Date(currentTime), new Timestamp(currentTime)),
          Row("KEY2", new Date(currentTime), new Timestamp(currentTime)),
          Row("KEY3", new Date(currentTime), new Timestamp(currentTime)))), anyStructType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName2, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY2", new Date(currentTime - 1000000000L), new Timestamp(currentTime - 1000L)))), anyStructType)
      dbCtl.updateRecords(delDf, tableName2, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName2).sort("KEY").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      d2s(result(0).getAs[Timestamp]("DDATE")) mustBe d2s(currentTime)
      result(0).getAs[Timestamp]("DTIMESTAMP").getTime mustBe currentTime
      result(1).getAs[String]("KEY") mustBe "KEY2"
      d2s(result(1).getAs[Timestamp]("DDATE")) mustBe d2s(currentTime - 1000000000L)
      result(1).getAs[Timestamp]("DTIMESTAMP").getTime mustBe (currentTime - 1000L)
      result(2).getAs[String]("KEY") mustBe "KEY3"
      d2s(result(2).getAs[Timestamp]("DDATE")) mustBe d2s(currentTime)
      result(2).getAs[Timestamp]("DTIMESTAMP").getTime mustBe currentTime
    }

    def testDate(day: Int) = DateTime.parse(s"2016-1-${day}").getMillis
    "update one rec. over 5 columns" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("AAA", "AAA2", "AAA3", "AAA4", "AAA5", new Date(testDate(1)), new Timestamp(testDate(10))),
          Row("BBB", "BBB2", "BBB3", "BBB4", "BBB5", new Date(testDate(2)), new Timestamp(testDate(20))),
          Row("CCC", "CCC2", "CCC3", "CCC4", "CCC5", new Date(testDate(3)), new Timestamp(testDate(30))))), structType5)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.autoCreateTable(tableName5)
        df.writeTable(tableName5, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val updDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("BBB", "BBB2", "BBB3", "BBB4", "BBB5",
          new Date(testDate(15)), new Timestamp(testDate(25))))), structType5)
      dbCtl.updateRecords(updDf, tableName5, Set("TEST", "TEST2", "TEST3", "TEST4", "TEST5"), Set.empty, "")

      def day(date: Timestamp) = new DateTime(date.getTime).getDayOfMonth
      val result = dbCtl.readTable(tableName5).sort("TEST").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("TEST") mustBe "AAA"
      day(result(0).getAs[Timestamp]("DDATE")) mustBe 1
      day(result(0).getAs[Timestamp]("DTIMESTAMP")) mustBe 10
      result(1).getAs[String]("TEST") mustBe "BBB"
      day(result(1).getAs[Timestamp]("DDATE")) mustBe 15
      day(result(1).getAs[Timestamp]("DTIMESTAMP")) mustBe 25
      result(2).getAs[String]("TEST") mustBe "CCC"
      day(result(2).getAs[Timestamp]("DDATE")) mustBe 3
      day(result(2).getAs[Timestamp]("DTIMESTAMP")) mustBe 30
    }

    val tblName7 = "col7"
    "update one rec. over 5 columns ignore keys add #10943" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", "a01", "a02", "a03", "a04", "a05", "a06"),
          Row("KEY2", "b01", "b02", "b03", "b04", "b05", "b07"))), stringStructType7Columns)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.autoCreateTable(tblName7)
        df.writeTable(tblName7, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val updDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "x01", "x02", "x03", "x04", "x05", "x06"),
        Row("KEY2", "y01", "y02", "y03", "y04", "y05", null))), stringStructType7Columns)
      dbCtl.updateRecords(updDf, tblName7, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tblName7).sort("KEY").collect.toList
      result.length mustBe 2
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("V1") mustBe "x01"
      result(0).getAs[String]("V2") mustBe "x02"
      result(0).getAs[String]("V3") mustBe "x03"
      result(0).getAs[String]("V4") mustBe "x04"
      result(0).getAs[String]("V5") mustBe "x05"
      result(0).getAs[String]("V6") mustBe "x06"

      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("V1") mustBe "y01"
      result(1).getAs[String]("V2") mustBe "y02"
      result(1).getAs[String]("V3") mustBe "y03"
      result(1).getAs[String]("V4") mustBe "y04"
      result(1).getAs[String]("V5") mustBe "y05"
      result(1).getAs[String]("V6") mustBe null
    }

    "update multi keys" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", new Date(currentTime - 9000000000L), new Timestamp(currentTime)),
          Row("KEY1", new Date(currentTime - 1000000000L), new Timestamp(currentTime)),
          Row("KEY1", new Date(currentTime), new Timestamp(currentTime)))), anyStructType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName2, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", new Date(currentTime - 1000000000L), new Timestamp(currentTime - 1000L)),
        Row("KEY1", new Date(currentTime - 9000000000L), new Timestamp(currentTime - 2000L)))), anyStructType)
      dbCtl.updateRecords(delDf, tableName2, Set("KEY", "DDATE"), Set.empty, "")

      val result = dbCtl.readTable(tableName2).sort("KEY", "DDATE").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[Timestamp]("DTIMESTAMP").getTime mustBe (currentTime - 2000L)
      result(1).getAs[String]("KEY") mustBe "KEY1"
      result(1).getAs[Timestamp]("DTIMESTAMP").getTime mustBe (currentTime - 1000L)
      result(2).getAs[String]("KEY") mustBe "KEY1"
      result(2).getAs[Timestamp]("DTIMESTAMP").getTime mustBe currentTime
    }

    "update all rec" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", "a1", "a2"), Row("KEY2", "b1", "b2"), Row("KEY3", "c1", "c2"))), stringStructType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx1", "yyy1"), Row("KEY2", "xxx2", "yyy2"), Row("KEY3", "xxx3", "yyy3"))), stringStructType)
      dbCtl.updateRecords(delDf, tableName, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName).collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("V1") mustBe "xxx1"
      result(0).getAs[String]("V2") mustBe "yyy1"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("V1") mustBe "xxx2"
      result(1).getAs[String]("V2") mustBe "yyy2"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("V1") mustBe "xxx3"
      result(2).getAs[String]("V2") mustBe "yyy3"
    }

    "update deleted one column" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", "a1", "a2"), Row("KEY2", "b1", "b2"), Row("KEY3", "c1", "c2"))), stringStructType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx1", "yyy1"), Row("KEY2", "xxx2", "yyy2"), Row("KEY3", "xxx3", "yyy3"))), stringStructType)
        .drop("V1")
      dbCtl.updateRecords(delDf, tableName, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName).collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("V1") mustBe "a1"
      result(0).getAs[String]("V2") mustBe "yyy1"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("V1") mustBe "b1"
      result(1).getAs[String]("V2") mustBe "yyy2"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("V1") mustBe "c1"
      result(2).getAs[String]("V2") mustBe "yyy3"
    }

    "nothing" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", "a1", "a2"), Row("KEY2", "b1", "b2"), Row("KEY3", "c1", "c2"))), stringStructType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY4", "xxx1", "yyy1"), Row("KEY5", "xxx2", "yyy2"), Row("KEY6", "xxx3", "yyy3"))), stringStructType)
      dbCtl.updateRecords(delDf, tableName, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName).collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("V1") mustBe "a1"
      result(0).getAs[String]("V2") mustBe "a2"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("V1") mustBe "b1"
      result(1).getAs[String]("V2") mustBe "b2"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("V1") mustBe "c1"
      result(2).getAs[String]("V2") mustBe "c2"
    }

    "be normal end. null pattern test." in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", null, null, null, null, null, null))), structTypeAnyType)
      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertAccelerated(df, "insertAcc", SaveMode.Overwrite)
      val df2 = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", null, null, null, null, null, null))), structTypeAnyType)

      dbCtl.updateRecords(df2, "insertAcc", Set("KEY1"), Set.empty, "")
      val result = dbCtl.readTable("insertAcc").sort("KEY1").collect.toList
      result.length mustBe 1
      result(0).getAs[String]("KEY1") mustBe "KEY1"
      result(0).getAs[Date]("KEY2") mustBe null
      result(0).getAs[Timestamp]("KEY3") mustBe null
      result(0).getAs[Integer]("KEY4") mustBe null
      result(0).getAs[Number]("KEY5") mustBe null
      result(0).getAs[Decimal]("KEY6") mustBe null
      result(0).getAs[String]("VALUE") mustBe null
    }

    "be normal end. null pattern no type test." in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", null, null, null, null, null, null))), structTypeAnyType)
      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertAccelerated(df, "insertAcc", SaveMode.Overwrite)
      val df2 = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", null, null, null, null, null, null))), structTypeAnyType)
        .drop("KEY2").drop("KEY3").drop("KEY4").drop("KEY5").drop("KEY6").drop("VALUE")
        .withColumn("KEY2", lit(null))
        .withColumn("KEY3", lit(null))
        .withColumn("KEY4", lit(null))
        .withColumn("KEY5", lit(null))
        .withColumn("KEY6", lit(null))
        .withColumn("VALUE", lit(null))

      dbCtl.updateRecords(df2, "insertAcc", Set("KEY1"), Set.empty, "")
      val result = dbCtl.readTable("insertAcc").sort("KEY1").collect.toList
      result.length mustBe 1
      result(0).getAs[Date]("KEY2") mustBe null
      result(0).getAs[Timestamp]("KEY3") mustBe null
      result(0).getAs[Integer]("KEY4") mustBe null
      result(0).getAs[Number]("KEY5") mustBe null
      result(0).getAs[Decimal]("KEY6") mustBe null
      result(0).getAs[String]("VALUE") mustBe null
    }
  }

  "DbCtl.upsertRecords" should {
    val tableName = "upsertTest1"
    val tableName2 = "upsertTest2"
    val structType = StructType(Seq(
      StructField("KEY", StringType),
      StructField("V1", StringType),
      StructField("V2", StringType)))

    val anyStructType = StructType(Seq(
      StructField("KEY", StringType),
      StructField("DDATE", DateType),
      StructField("DTIMESTAMP", TimestampType)))
    "update one rec" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", new Date(currentTime), new Timestamp(currentTime)),
          Row("KEY2", new Date(currentTime), new Timestamp(currentTime)),
          Row("KEY3", new Date(currentTime), new Timestamp(currentTime)))), anyStructType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName2, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY2", new Date(currentTime - 1000000000L), new Timestamp(currentTime - 1000L)))), anyStructType)
      dbCtl.upsertRecords(delDf, tableName2, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName2).sort("KEY").collect.toList
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      d2s(result(0).getAs[Timestamp]("DDATE")) mustBe d2s(currentTime)
      result(0).getAs[Timestamp]("DTIMESTAMP").getTime mustBe currentTime
      result(1).getAs[String]("KEY") mustBe "KEY2"
      d2s(result(1).getAs[Timestamp]("DDATE")) mustBe d2s(currentTime - 1000000000L)
      result(1).getAs[Timestamp]("DTIMESTAMP").getTime mustBe (currentTime - 1000L)
      result(2).getAs[String]("KEY") mustBe "KEY3"
      d2s(result(2).getAs[Timestamp]("DDATE")) mustBe d2s(currentTime)
      result(2).getAs[Timestamp]("DTIMESTAMP").getTime mustBe currentTime
    }

    "update multi keys" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", "a1", "a2"), Row("KEY1", "b1", "b2"), Row("KEY1", "c1", "c2"))), structType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "a1", "yyy"))), structType)
      dbCtl.upsertRecords(delDf, tableName, Set("KEY", "V1"), Set.empty, "")

      val result = dbCtl.readTable(tableName).collect.toList.sortBy(x => (x.getAs[String]("KEY"), x.getAs[String]("V1")))
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("V1") mustBe "a1"
      result(0).getAs[String]("V2") mustBe "yyy"
      result(1).getAs[String]("KEY") mustBe "KEY1"
      result(1).getAs[String]("V1") mustBe "b1"
      result(1).getAs[String]("V2") mustBe "b2"
      result(2).getAs[String]("KEY") mustBe "KEY1"
      result(2).getAs[String]("V1") mustBe "c1"
      result(2).getAs[String]("V2") mustBe "c2"
    }

    "update deleted one column and Insert" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", "a1", "a2"), Row("KEY2", "b1", "b2"), Row("KEY3", "c1", "c2"))), structType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx1", "yyy1"), Row("KEY2", "xxx2", "yyy2"), Row("KEY3", "xxx3", "yyy3"), Row("KEY4", "xxx4", "yyy4"))), structType)
        .drop("v1")
      dbCtl.upsertRecords(delDf, tableName, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName).collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 4
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("V1") mustBe "a1"
      result(0).getAs[String]("V2") mustBe "yyy1"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("V1") mustBe "b1"
      result(1).getAs[String]("V2") mustBe "yyy2"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("V1") mustBe "c1"
      result(2).getAs[String]("V2") mustBe "yyy3"
      result(3).getAs[String]("KEY") mustBe "KEY4"
      result(3).getAs[String]("V1") mustBe " "
      result(3).getAs[String]("V2") mustBe "yyy4"
    }

    "update all rec" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", "a1", "a2"), Row("KEY2", "b1", "b2"), Row("KEY3", "c1", "c2"))), structType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx1", "yyy1"), Row("KEY2", "xxx2", "yyy2"), Row("KEY3", "xxx3", "yyy3"))), structType)
      dbCtl.upsertRecords(delDf, tableName, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName).collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("V1") mustBe "xxx1"
      result(0).getAs[String]("V2") mustBe "yyy1"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("V1") mustBe "xxx2"
      result(1).getAs[String]("V2") mustBe "yyy2"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("V1") mustBe "xxx3"
      result(2).getAs[String]("V2") mustBe "yyy3"
    }

    "update ignore column" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", "a1", "a2"), Row("KEY2", "b1", "b2"), Row("KEY3", "c1", "c2"))), structType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx1", "yyy1"), Row("KEY2", "xxx2", "yyy2"), Row("KEY3", "xxx3", "yyy3"))), structType)
      dbCtl.upsertRecords(delDf, tableName, Set("KEY"), Set("V1"), "")

      val result = dbCtl.readTable(tableName).collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("V1") mustBe "a1"
      result(0).getAs[String]("V2") mustBe "yyy1"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("V1") mustBe "b1"
      result(1).getAs[String]("V2") mustBe "yyy2"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("V1") mustBe "c1"
      result(2).getAs[String]("V2") mustBe "yyy3"
    }

    "insert" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", new Date(currentTime), new Timestamp(currentTime)))), anyStructType)

        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName2, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY2", new Date(currentTime + 1000L), new Timestamp(currentTime + 10000000000L)))), anyStructType)
      dbCtl.upsertRecords(delDf, tableName2, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName2).sort("KEY").collect.toList
      result.length mustBe 2
      result(0).getAs[String]("KEY") mustBe "KEY1"
      d2s(result(0).getAs[Timestamp]("DDATE")) mustBe d2s(currentTime)
      result(0).getAs[Timestamp]("DTIMESTAMP").getTime mustBe (currentTime)
      result(1).getAs[String]("KEY") mustBe "KEY2"
      d2s(result(1).getAs[Timestamp]("DDATE")) mustBe d2s(currentTime + 1000L)
      result(1).getAs[Timestamp]("DTIMESTAMP").getTime mustBe (currentTime + 10000000000L)
    }

    "throw db exception" in {
      val structType = StructType(Seq(
        StructField("KEY", StringType),
        StructField("nullCol", StringType)))

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY01", "AAA"), Row("KEY02", null))), structType)
      try {
        dbCtl.upsertRecords(delDf, "nulltest", Set("KEY"), Set.empty, "")
        fail()
      } catch {
        case t: Throwable => t.printStackTrace()
      }
    }

    "be normal end. insert null pattern test." in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", null, null, null, null, null, null))), structTypeAnyType)
      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertAccelerated(df, "insertAcc", SaveMode.Overwrite)
      val df2 = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY2", null, null, null, null, null, null))), structTypeAnyType)

      dbCtl.upsertRecords(df2, "insertAcc", Set("KEY1"), Set.empty, "")
      val result = dbCtl.readTable("insertAcc").sort("KEY1").collect.toList
      result.length mustBe 2
      result(1).getAs[String]("KEY1") mustBe "KEY2"
      result(1).getAs[Date]("KEY2") mustBe null
      result(1).getAs[Timestamp]("KEY3") mustBe null
      result(1).getAs[Integer]("KEY4") mustBe null
      result(1).getAs[Number]("KEY5") mustBe null
      result(1).getAs[Decimal]("KEY6") mustBe null
      result(1).getAs[String]("VALUE") mustBe null
    }

    "be normal end. update null pattern test." in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", null, null, null, null, null, null))), structTypeAnyType)
      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertAccelerated(df, "insertAcc", SaveMode.Overwrite)
      val df2 = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", null, null, null, null, null, null))), structTypeAnyType)

      dbCtl.upsertRecords(df2, "insertAcc", Set("KEY1"), Set.empty, "")
      val result = dbCtl.readTable("insertAcc").sort("KEY1").collect.toList
      result.length mustBe 1
      result(0).getAs[String]("KEY1") mustBe "KEY1"
      result(0).getAs[Date]("KEY2") mustBe null
      result(0).getAs[Timestamp]("KEY3") mustBe null
      result(0).getAs[Integer]("KEY4") mustBe null
      result(0).getAs[Number]("KEY5") mustBe null
      result(0).getAs[Decimal]("KEY6") mustBe null
      result(0).getAs[String]("VALUE") mustBe null
    }

    "be normal end. null pattern no type test." in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", null, null, null, null, null, null))), structTypeAnyType)
      val dbCtl = new DbCtl(dbtarget)
      dbCtl.insertAccelerated(df, "insertAcc", SaveMode.Overwrite)
      val df2 = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("key1", null, null, null, null, null, null))), structTypeAnyType)
        .drop("KEY2").drop("KEY3").drop("KEY4").drop("KEY5").drop("KEY6").drop("VALUE")
        .withColumn("KEY2", lit(null))
        .withColumn("KEY3", lit(null))
        .withColumn("KEY4", lit(null))
        .withColumn("KEY5", lit(null))
        .withColumn("KEY6", lit(null))
        .withColumn("VALUE", lit(null))

      dbCtl.upsertRecords(df2, "insertAcc", Set("KEY1"), Set.empty, "")
      val result = dbCtl.readTable("insertAcc").sort("KEY1").collect.toList
      result.length mustBe 1
      result(0).getAs[Date]("KEY2") mustBe null
      result(0).getAs[Timestamp]("KEY3") mustBe null
      result(0).getAs[Integer]("KEY4") mustBe null
      result(0).getAs[Number]("KEY5") mustBe null
      result(0).getAs[Decimal]("KEY6") mustBe null
      result(0).getAs[String]("VALUE") mustBe null
    }

  }

  "DateType Test" should {
    val tableName1 = "datetype"
    val structType = StructType(Seq(
      StructField("KEY", StringType), StructField("DDATE", DateType)))
    "insert" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", new Date(currentTime)))), structType)
      val dbCtl = new DbCtl(dbtarget)
      import dbCtl.implicits._
      df.writeTable(tableName1, SaveMode.Overwrite)

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 1
      d2s(result(0).getAs[Timestamp]("DDATE")) mustBe d2s(currentTime)
    }

    "update" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", new Date(currentTime)))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", new Date(currentTime - 1000L)))), structType)
      dbCtl.updateRecords(delDf, tableName1, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 1
      d2s(result(0).getAs[Timestamp]("DDATE")) mustBe d2s(currentTime - 1000L)
    }

    "upsert" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", new Date(currentTime)))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", new Date(currentTime - 1000L)),
        Row("KEY2", new Date(currentTime - 2000L)))), structType)
      dbCtl.upsertRecords(delDf, tableName1, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 2
      result(0).getAs[String]("KEY") mustBe "KEY1"
      d2s(result(0).getAs[Timestamp]("DDATE")) mustBe d2s(currentTime - 1000L)
      result(1).getAs[String]("KEY") mustBe "KEY2"
      d2s(result(1).getAs[Timestamp]("DDATE")) mustBe d2s(currentTime - 2000L)
    }

    "delete" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", new Date(currentTime)))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", new Date(currentTime)))), structType)
      dbCtl.deleteRecords(delDf, tableName1, Set("DDATE"))

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 0
    }
  }

  "TimestampTypeTest" should {
    val tableName1 = "DTIMESTAMP"
    val structType = StructType(Seq(
      StructField("KEY", StringType), StructField("DTIMESTAMP", TimestampType)))
    "insert" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", new Timestamp(currentTime)))), structType)
      val dbCtl = new DbCtl(dbtarget)
      import dbCtl.implicits._
      df.writeTable(tableName1, SaveMode.Overwrite)

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 1
      d2s(result(0).getAs[Timestamp]("DTIMESTAMP")) mustBe d2s(currentTime)
    }

    "update" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", new Timestamp(currentTime)))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", new Timestamp(currentTime - 1000L)))), structType)
      dbCtl.updateRecords(delDf, tableName1, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 1
      d2s(result(0).getAs[Timestamp]("DTIMESTAMP")) mustBe d2s(currentTime - 1000L)
    }

    "upsert" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", new Timestamp(currentTime)))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", new Timestamp(currentTime - 1000L)),
        Row("KEY2", new Timestamp(currentTime - 2000L)))), structType)
      dbCtl.upsertRecords(delDf, tableName1, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 2
      result(0).getAs[String]("KEY") mustBe "KEY1"
      d2s(result(0).getAs[Timestamp]("DTIMESTAMP")) mustBe d2s(currentTime - 1000L)
      result(1).getAs[String]("KEY") mustBe "KEY2"
      d2s(result(1).getAs[Timestamp]("DTIMESTAMP")) mustBe d2s(currentTime - 2000L)
    }

    "delete" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", new Timestamp(currentTime)))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", new Timestamp(currentTime)))), structType)
      dbCtl.deleteRecords(delDf, tableName1, Set("DTIMESTAMP"))

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 0
    }
  }

  "DecimalType Test" should {
    val tableName1 = "decimaltype"
    val structType = StructType(Seq(
      StructField("KEY", StringType), StructField("DDECIMAL", DecimalType(8, 0))))
    "insert" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", BigDecimal(100).bigDecimal))), structType)
      val dbCtl = new DbCtl(dbtarget)
      import dbCtl.implicits._
      df.writeTable(tableName1, SaveMode.Overwrite)

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 1
      result(0).getAs[java.math.BigDecimal]("DDECIMAL") mustBe BigDecimal(100).bigDecimal
    }

    "update" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", BigDecimal(100).bigDecimal))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", BigDecimal(200).bigDecimal))), structType)
      dbCtl.updateRecords(delDf, tableName1, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 1
      result(0).getAs[java.math.BigDecimal]("DDECIMAL") mustBe BigDecimal(200).bigDecimal
    }

    "upsert" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", BigDecimal(100).bigDecimal))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", BigDecimal(200).bigDecimal),
        Row("KEY2", BigDecimal(300).bigDecimal))), structType)
      dbCtl.upsertRecords(delDf, tableName1, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 2
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[java.math.BigDecimal]("DDECIMAL") mustBe BigDecimal(200).bigDecimal
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[java.math.BigDecimal]("DDECIMAL") mustBe BigDecimal(300).bigDecimal
    }

    "delete" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", BigDecimal(100).bigDecimal))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", BigDecimal(100).bigDecimal))), structType)
      dbCtl.deleteRecords(delDf, tableName1, Set("DDECIMAL"))

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 0
    }
  }

  "IntegerType Test" should {
    val tableName1 = "integertype"
    val structType = StructType(Seq(
      StructField("KEY", StringType), StructField("DINTEGER", IntegerType)))
    "insert" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", 100))), structType)
      val dbCtl = new DbCtl(dbtarget)
      import dbCtl.implicits._
      df.writeTable(tableName1, SaveMode.Overwrite)

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 1
      result(0).getAs[java.math.BigDecimal]("DINTEGER").toString mustBe "100"
    }

    "update" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", 100))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", 200))), structType)
      dbCtl.updateRecords(delDf, tableName1, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 1
      result(0).getAs[java.math.BigDecimal]("DINTEGER").toString mustBe "200"
    }

    "upsert" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", 100))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", 200),
        Row("KEY2", 300))), structType)
      dbCtl.upsertRecords(delDf, tableName1, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 2
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[java.math.BigDecimal]("DINTEGER").toString mustBe "200"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[java.math.BigDecimal]("DINTEGER").toString mustBe "300"
    }

    "delete" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", 100))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", 100))), structType)
      dbCtl.deleteRecords(delDf, tableName1, Set("DINTEGER"))

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 0
    }
  }

  "LongType Test" should {
    val tableName1 = "LONGTYPE"
    val structType = StructType(Seq(
      StructField("KEY", StringType), StructField("DLONG", LongType)))
    "insert" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", 100L))), structType)
      val dbCtl = new DbCtl(dbtarget)
      import dbCtl.implicits._
      df.writeTable(tableName1, SaveMode.Overwrite)

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 1
      result(0).getAs[Long]("DLONG").toString mustBe "100.0000000000"
    }

    "update" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", 100L))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", 200L))), structType)
      dbCtl.updateRecords(delDf, tableName1, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 1
      result(0).getAs[Long]("DLONG").toString mustBe "200.0000000000"
    }

    "upsert" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", 100L))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", 200L),
        Row("KEY2", 300L))), structType)
      dbCtl.upsertRecords(delDf, tableName1, Set("KEY"), Set.empty, "")

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 2
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[Long]("DLONG").toString mustBe "200.0000000000"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[Long]("DLONG").toString mustBe "300.0000000000"
    }

    "delete" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("KEY1", 100L))), structType)
        val dbCtl = new DbCtl(dbtarget)
        import dbCtl.implicits._
        df.writeTable(tableName1, SaveMode.Overwrite)
      }

      val dbCtl = new DbCtl(dbtarget)
      val delDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", 100L))), structType)
      dbCtl.deleteRecords(delDf, tableName1, Set("DLONG"))

      val result = dbCtl.readTable(tableName1).sort("KEY").collect.toList
      result.length mustBe 0
    }
  }

  "DbCtl.writeTable" should {
    val tableName = "notExist"
    val structType = StructType(Seq(
      StructField("test", StringType)))
    "not Exist Table for Overwrite" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("AAA"), Row("BBB"), Row("CCC"))), structType)

        val dbCtl = new DbCtl
        import dbCtl.implicits._
        try {
          df.writeTable(tableName, SaveMode.Overwrite)
          fail
        } catch {
          case t: Throwable => t.printStackTrace()
        }
      }
    }

    "not Exist Table for Append" in {
      {
        val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
          Row("AAA"), Row("BBB"), Row("CCC"))), structType)

        val dbCtl = new DbCtl
        import dbCtl.implicits._
        try {
          df.writeTable(tableName, SaveMode.Overwrite)
          fail
        } catch {
          case t: Throwable => t.printStackTrace()
        }
      }
    }
  }

  "DbCtl readTable" should {
    "be success require column mode read all data　on single thread" in {
      val dbCtl = new DbCtl
      val tableName = "SP01"
      val columns = Array("DT", "NUM5", "NUM52", "TSTMP", "VC", "CH")
      val result = dbCtl.readTable(tableName, columns, DbCtl.readAllData).collect
      result.size mustBe 4
      result.head.schema.size mustBe 6
      val names = result.head.schema.map(_.name)
      names.exists(_ == "DT") mustBe true
      names.exists(_ == "NUM5") mustBe true
      names.exists(_ == "NUM52") mustBe true
      names.exists(_ == "TSTMP") mustBe true
      names.exists(_ == "VC") mustBe true
      names.exists(_ == "CH") mustBe true

      result.size mustBe 4
      result(0).getAs[java.math.BigDecimal]("NUM5").toString mustBe "1000"
      result(1).getAs[java.math.BigDecimal]("NUM5").toString mustBe "2000"
      result(2).getAs[java.math.BigDecimal]("NUM5").toString mustBe "3000"
    }

    "be success require column mode using where" in {
      val dbCtl = new DbCtl
      val tableName = "SP01"
      val columns = Array("DT", "NUM5", "NUM52", "TSTMP", "VC", "CH")
      val where = Array("NUM5 = '1000'", "NUM5 = '2000'")
      val result = dbCtl.readTable(tableName, columns, where).collect
      result.size mustBe 2
      result.head.schema.size mustBe 6
      val names = result.head.schema.map(_.name)
      names.exists(_ == "DT") mustBe true
      names.exists(_ == "NUM5") mustBe true
      names.exists(_ == "NUM52") mustBe true
      names.exists(_ == "TSTMP") mustBe true
      names.exists(_ == "VC") mustBe true
      names.exists(_ == "CH") mustBe true

      result.size mustBe 2
      result(0).getAs[java.math.BigDecimal]("NUM5").toString mustBe "1000"
      result(1).getAs[java.math.BigDecimal]("NUM5").toString mustBe "2000"
    }
  }

  "DbCtl truncateTable" should {
    val tableOwner = "TRANCATE_TEST_USER"
    val tableName1 = "TRANCATE_TEST_TABLE1"
    val tableName2 = "TRANCATE_TEST_TABLE2"
    val tableName3 = "TRANCATE_TEST_TABLE3"
    val errTableName = "DELETE_TEST_TABLE"
    val dbCtl = new DbCtl

    before {
      Try {
        dbCtl.execSql(tableOwner, s"CREATE USER ${tableOwner} IDENTIFIED BY ${tableOwner}")
      }

      Try {
        dbCtl.execSql(tableName1, s"CREATE TABLE ${tableName1} AS SELECT * FROM DUAL")
        dbCtl.execSql(tableName1, s"CREATE SYNONYM ${tableName1} FOR ${tableOwner}.${tableName1}")
        dbCtl.execSql(tableName1, s"CREATE PUBLIC SYNONYM ${tableName1} FOR ${tableOwner}.${tableName1}")
      }

      Try {
        dbCtl.execSql(tableName2, s"CREATE TABLE ${tableName2} AS SELECT * FROM DUAL")
      }

      Try {
        dbCtl.execSql(tableName3, s"CREATE TABLE ${tableName3} AS SELECT * FROM DUAL")
        dbCtl.execSql(tableName3, s"CREATE SYNONYM ${tableName3} FOR ${tableOwner}.${tableName3}")
        dbCtl.execSql(tableName3, s"CREATE PUBLIC SYNONYM ${tableName3} FOR ${tableOwner}.${tableName3}")
      }
    }

    "exists SYNONYM" in {
      dbCtl.clearTable(tableName1)
    }

    "not exists SYNONYM" in {
      dbCtl.clearTable(tableName2)
    }

    "Owner.TableName" in {
      dbCtl.clearTable(tableName3)
    }

    "delete execution" in {
      intercept[Exception] {
        dbCtl.clearTable(errTableName)
      }
    }
  }

  "DbCtl columnTypes" should {
    "be success" in {
      val props = new Properties
      props.put("user", dbtarget.user)
      props.put("password", dbtarget.password)
      props.put("charSet", dbtarget.charSet)
      val conn = JdbcUtils.createConnectionFactory(dbtarget.toOptions)()

      val dbCtl = new DbCtl(dbtarget)
      val result = dbCtl.columnTypes(conn, "SP01")
      println(result)
      import java.sql.{ Types => t }
      result("NUM52") mustBe t.DECIMAL
      result("VC") mustBe t.VARCHAR
      result("TSTMP") mustBe t.TIMESTAMP
      result("NUM5") mustBe t.DECIMAL
      result("CH") mustBe t.CHAR
      result("DT") mustBe t.TIMESTAMP
    }
  }
}
