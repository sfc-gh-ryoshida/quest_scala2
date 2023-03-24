package d2k.common.df

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.common.TestArgs
import d2k.common.InputArgs

import java.sql.Date
import org.joda.time.DateTime
import java.sql.Timestamp

import spark.common.SparkContexts.context
import context.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spark.common.SparkContexts
import spark.common.DbCtl
import d2k.common.df.WriteDbMode._
import org.apache.spark.sql.SaveMode
import java.time.LocalDateTime

case class Sp01(DT: Timestamp, NUM5: BigDecimal, NUM52: BigDecimal, TSTMP: Timestamp, VC: String, CH: String)
class WriteDbTest extends WordSpec with MustMatchers with BeforeAndAfter {
  def d2s(dateMill: Long) = new DateTime(dateMill).toString("yyyy-MM-dd")
  def d2s(date: Date) = new DateTime(date).toString("yyyy-MM-dd")
  def d2s(date: Timestamp) = new DateTime(date).toString("yyyy-MM-dd hh:mm:ss")

  implicit val inArgs = TestArgs().toInputArgs
  val structType = StructType(Seq(
    StructField("KEY", StringType), StructField("TEST", StringType)))
  val dbCtl = new DbCtl()
  import dbCtl.implicits._
  import context.implicits._

  "Insert" should {
    "be normal end" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq.empty[Row]), structType)
      DbCommonColumnAppender(df, "CS_TEST").autoCreateTable("CS_TEST")
      val target = new WriteDb {
        val componentId = "CS_TEST"
        override val writeDbMode = Insert
      }
      val insertDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"))), structType)
      target.writeDb(insertDf)

      val result = dbCtl.readTable("CS_TEST").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 1

      result(0).getAs[String]("TEST") mustBe "aaa"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
    }

    "insert not set CommonColumns" in {
      val target = new WriteDb {
        val componentId = "insertNotSetCommonColumns"
        override val writeDbMode = Insert
        override val writeDbWithCommonColumn = false
        override val writeDbSaveMode = SaveMode.Overwrite
      }

      implicit val inArgs = TestArgs().toInputArgs
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      target.writeDb(df)

      val dbCtl = new DbCtl()
      val schemas = dbCtl.readTable("insertNotSetCommonColumns").schema
      schemas.contains("DT_D2KMKDTTM") mustBe false
    }

    "be normal end　with CommitSize" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq.empty[Row]), structType)
      DbCommonColumnAppender(df, "CS_TEST").autoCreateTable("CS_TEST")
      val target = new WriteDb {
        val componentId = "CS_TEST"
        override val writeDbMode = Insert
        override val writeDbInfo = DbConnectionInfo.bat1.copy(commitSize = Some(2))
      }
      val insertDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"))), structType)
      target.writeDb(insertDf)

      val result = dbCtl.readTable("CS_TEST").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 1

      result(0).getAs[String]("TEST") mustBe "aaa"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
    }
  }

  "InsertAcc" should {
    "be normal end" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq.empty[Row]), structType)
      DbCommonColumnAppender(df, "CS_TEST").autoCreateTable("CS_TEST")
      val target = new WriteDb {
        val componentId = "CS_TEST"
        override val writeDbMode = InsertAcc
      }
      val insertDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"))), structType)
      target.writeDb(insertDf)

      val result = dbCtl.readTable("CS_TEST").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 1

      result(0).getAs[String]("TEST") mustBe "aaa"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
    }

    "insert not set CommonColumns" in {
      val target = new WriteDb {
        val componentId = "insertNotSetCommonColumns"
        override val writeDbMode = InsertAcc
        override val writeDbWithCommonColumn = false
        override val writeDbSaveMode = SaveMode.Overwrite
      }

      implicit val inArgs = TestArgs().toInputArgs
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      target.writeDb(df)

      val dbCtl = new DbCtl()
      val schemas = dbCtl.readTable("insertNotSetCommonColumns").schema
      schemas.contains("DT_D2KMKDTTM") mustBe false
    }

    "be normal end　with commitSize" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq.empty[Row]), structType)
      DbCommonColumnAppender(df, "CS_TEST").autoCreateTable("CS_TEST")
      val target = new WriteDb {
        val componentId = "CS_TEST"
        override val writeDbMode = InsertAcc
        override val writeDbInfo = DbConnectionInfo.bat1.copy(commitSize = Some(2))
      }
      val insertDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"))), structType)
      target.writeDb(insertDf)

      val result = dbCtl.readTable("CS_TEST").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 1

      result(0).getAs[String]("TEST") mustBe "aaa"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
    }
  }

  "InsertNotExist" should {
    "be normal end" in {
      val target = new WriteDb {
        val componentId = "insertNotExist"
        override val writeDbMode = InsertNotExists("KEY")
        override val writeDbWithCommonColumn = true
        override val writeDbSaveMode = SaveMode.Overwrite
      }
      val insertDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY1", "bbb"), Row("KEY2", "ccc"))), structType)
      target.writeDb(insertDf.repartition(1))
      val result = dbCtl.readTable("insertNotExist").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 2

      result(0).getAs[String]("TEST") mustBe "aaa"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "insertNotExist"

      result(1).getAs[String]("TEST") mustBe "ccc"
    }

    "append" in {
      val target = new WriteDb {
        val componentId = "insertNotExist"
        override val writeDbMode = InsertNotExists("KEY")
        override val writeDbSaveMode = SaveMode.Overwrite
      }
      val insertDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY1", "bbb"))), structType)
      target.writeDb(insertDf.repartition(1))

      val target2 = new WriteDb {
        val componentId = "insertNotExist"
        override val writeDbMode = InsertNotExists("KEY")
        override val writeDbSaveMode = SaveMode.Append
      }
      val insertDf2 = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY2", "ccc"), Row("KEY2", "ddd"))), structType)
      target2.writeDb(insertDf2.repartition(1))

      val result = dbCtl.readTable("insertNotExist").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 2

      result(0).getAs[String]("TEST") mustBe "aaa"
      result(1).getAs[String]("TEST") mustBe "ccc"
    }

    "InsertNotExist not set CommonColumns" in {
      val target = new WriteDb {
        val componentId = "insertNotExistNotSetColumns"
        override val writeDbMode = InsertNotExists("KEY")
        override val writeDbWithCommonColumn = false
        override val writeDbSaveMode = SaveMode.Overwrite
      }

      implicit val inArgs = TestArgs().toInputArgs
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY1", "bbb"))), structType)
      target.writeDb(df.repartition(1))

      val dbCtl = new DbCtl()
      val result = dbCtl.readTable("insertNotExistNotSetColumns").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 1

      result(0).getAs[String]("TEST") mustBe "aaa"
      result.contains("DT_D2KMKDTTM") mustBe false
    }

    "be normal end with commitSize" in {
      val target = new WriteDb {
        val componentId = "insertNotExist"
        override val writeDbMode = InsertNotExists("KEY")
        override val writeDbWithCommonColumn = true
        override val writeDbSaveMode = SaveMode.Overwrite
        override val writeDbInfo = DbConnectionInfo.bat1.copy(commitSize = Some(2))
      }
      val insertDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY1", "bbb"))), structType)
      target.writeDb(insertDf.repartition(1))

      val result = dbCtl.readTable("insertNotExist").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 1

      result(0).getAs[String]("TEST") mustBe "aaa"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "insertNotExist"
    }
  }

  val structTypeUpd = StructType(Seq(
    StructField("KEY", StringType), StructField("TEST", StringType), StructField("TEST2", StringType)))
  "Update" should {
    "be normal end" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      DbCommonColumnAppender(df, "compo").writeTable("CS_TEST", SaveMode.Overwrite)

      val target = new WriteDb {
        val componentId = "CS_TEST"
        override val writeDbMode = Update
        override val writeDbUpdateKeys = Set("KEY")
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx"), Row("KEY3", "yyy"))), structType)
      target.writeDb(updateDf)

      val result = dbCtl.readTable("CS_TEST").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("TEST") mustBe "xxx"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("TEST") mustBe "bbb"
      result(1).getAs[Timestamp]("DT_D2KUPDDTTM") ne inArgs.sysSQLDate
      result(1).getAs[String]("ID_D2KUPDUSR") mustBe "compo"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("TEST") mustBe "yyy"
      d2s(result(2).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(2).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
    }

    "Update not set CommonColumns" in {
      val insert = new WriteDb {
        val componentId = "updateNotSetCommonColumns"
        override val writeDbMode = Insert
        override val writeDbWithCommonColumn = false
        override val writeDbSaveMode = SaveMode.Overwrite
      }

      implicit val inArgs = TestArgs().toInputArgs
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      insert.writeDb(df)

      val target = new WriteDb {
        val componentId = "updateNotSetCommonColumns"
        override val writeDbMode = Update
        override val writeDbWithCommonColumn = false
        override val writeDbUpdateKeys = Set("KEY")
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx"), Row("KEY3", "yyy"))), structType)
      target.writeDb(updateDf)

      val dbCtl = new DbCtl()
      val schemas = dbCtl.readTable("updateNotSetCommonColumns").schema
      schemas.foreach(println)
      schemas.contains("DT_D2KMKDTTM") mustBe false
    }

    "Update ignore column　single" in {
      val dfdummy = context.createDataFrame(SparkContexts.sc.makeRDD(Seq.empty[Row]), structTypeUpd)
      DbCommonColumnAppender(dfdummy, "compo").autoCreateTable("cs_test_update_ignore")

      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa", "aaa2"), Row("KEY2", "bbb", "bbb2"), Row("KEY3", "ccc", "ccc2"))), structTypeUpd)
      DbCommonColumnAppender(df, "compo").writeTable("cs_test_update_ignore", SaveMode.Overwrite)

      val target = new WriteDb {
        val componentId = "cs_test_update_ignore"
        override val writeDbMode = Update
        override val writeDbUpdateKeys = Set("KEY")
        override val writeDbUpdateIgnoreColumns = Set("TEST2")
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx", "99999"), Row("KEY3", "yyy", "000000"))), structTypeUpd)
      target.writeDb(updateDf)

      val result = dbCtl.readTable("cs_test_update_ignore").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("TEST") mustBe "xxx"
      result(0).getAs[String]("TEST2") mustBe "aaa2"

      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("TEST") mustBe "bbb"
      result(1).getAs[String]("TEST2") mustBe "bbb2"

      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("TEST") mustBe "yyy"
      result(2).getAs[String]("TEST2") mustBe "ccc2"
    }

    "Update ignore column　multi" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa", "aaa2"), Row("KEY2", "bbb", "bbb2"), Row("KEY3", "ccc", "ccc2"))), structTypeUpd)
      DbCommonColumnAppender(df, "compo").writeTable("cs_test_update_ignore", SaveMode.Overwrite)

      val target = new WriteDb {
        val componentId = "cs_test_update_ignore"
        override val writeDbMode = Update
        override val writeDbUpdateKeys = Set("KEY")
        override val writeDbUpdateIgnoreColumns = Set("TEST", "TEST2")
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx", "99999"), Row("KEY3", "yyy", "000000"))), structTypeUpd)
      target.writeDb(updateDf)

      val result = dbCtl.readTable("cs_test_update_ignore").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("TEST") mustBe "aaa"
      result(0).getAs[String]("TEST2") mustBe "aaa2"

      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("TEST") mustBe "bbb"
      result(1).getAs[String]("TEST2") mustBe "bbb2"

      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("TEST") mustBe "ccc"
      result(2).getAs[String]("TEST2") mustBe "ccc2"
    }

    "be normal end with commitSize" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      DbCommonColumnAppender(df, "compo").writeTable("CS_TEST", SaveMode.Overwrite)

      val target = new WriteDb {
        val componentId = "CS_TEST"
        override val writeDbMode = Update
        override val writeDbUpdateKeys = Set("KEY")
        override val writeDbInfo = DbConnectionInfo.bat1.copy(commitSize = Some(2))
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx"), Row("KEY3", "yyy"))), structType)
      target.writeDb(updateDf)

      val result = dbCtl.readTable("CS_TEST").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("TEST") mustBe "xxx"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("TEST") mustBe "bbb"
      result(1).getAs[Timestamp]("DT_D2KUPDDTTM") ne inArgs.sysSQLDate
      result(1).getAs[String]("ID_D2KUPDUSR") mustBe "compo"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("TEST") mustBe "yyy"
      d2s(result(2).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(2).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
    }
  }

  "Upsert" should {
    "be normal end" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      DbCommonColumnAppender(df, "compo").writeTable("CS_TEST", SaveMode.Overwrite)

      val target = new WriteDb {
        val componentId = "CS_TEST"
        override val writeDbMode = Upsert
        override val writeDbUpdateKeys = Set("KEY")
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx"), Row("KEY3", "yyy"), Row("KEY4", "zzz"))), structType)
      target.writeDb(updateDf)

      val dbCtl = new DbCtl()
      val result = dbCtl.readTable("CS_TEST").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 4
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("TEST") mustBe "xxx"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("TEST") mustBe "bbb"
      result(1).getAs[Timestamp]("DT_D2KUPDDTTM") ne inArgs.sysSQLDate
      result(1).getAs[String]("ID_D2KUPDUSR") ne "CS_TEST"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("TEST") mustBe "yyy"
      d2s(result(2).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(2).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
      result(3).getAs[String]("KEY") mustBe "KEY4"
      result(3).getAs[String]("TEST") mustBe "zzz"
      d2s(result(3).getAs[Timestamp]("DT_D2KMKDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(3).getAs[String]("ID_D2KMKUSR") mustBe "CS_TEST"
      d2s(result(3).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(3).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
    }

    "Upsert not set CommonColumns" in {
      val insert = new WriteDb {
        val componentId = "upsertNotSetCommonColumns"
        override val writeDbMode = Insert
        override val writeDbWithCommonColumn = false
        override val writeDbSaveMode = SaveMode.Overwrite
      }

      implicit val inArgs = TestArgs().toInputArgs
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      insert.writeDb(df)

      val target = new WriteDb {
        val componentId = "upsertNotSetCommonColumns"
        override val writeDbMode = Upsert
        override val writeDbWithCommonColumn = false
        override val writeDbUpdateKeys = Set("KEY")
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx"), Row("KEY3", "yyy"))), structType)
      target.writeDb(updateDf)

      val dbCtl = new DbCtl()
      val schemas = dbCtl.readTable("upsertNotSetCommonColumns").schema
      schemas.foreach(println)
      schemas.contains("DT_D2KMKDTTM") mustBe false
    }

    "Upsert ignore column single" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa", "aaa2"), Row("KEY2", "bbb", "bbb2"), Row("KEY3", "ccc", "ccc2"))), structTypeUpd)
      DbCommonColumnAppender(df, "compo").writeTable("cs_test_update_ignore", SaveMode.Overwrite)

      val target = new WriteDb {
        val componentId = "cs_test_update_ignore"
        override val writeDbMode = Upsert
        override val writeDbUpdateKeys = Set("KEY")
        override val writeDbUpdateIgnoreColumns = Set("TEST2")
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx", "11111"), Row("KEY3", "yyy", "222222"), Row("KEY4", "zzz", "33333"))), structTypeUpd)
      target.writeDb(updateDf)

      val dbCtl = new DbCtl()
      val result = dbCtl.readTable("cs_test_update_ignore").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 4
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("TEST") mustBe "xxx"
      result(0).getAs[String]("TEST2") mustBe "aaa2"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("TEST") mustBe "bbb"
      result(1).getAs[String]("TEST2") mustBe "bbb2"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("TEST") mustBe "yyy"
      result(2).getAs[String]("TEST2") mustBe "ccc2"
      result(3).getAs[String]("KEY") mustBe "KEY4"
      result(3).getAs[String]("TEST") mustBe "zzz"
      result(3).getAs[String]("TEST2") mustBe "33333"
    }

    "Upsert ignore column multi" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa", "aaa2"), Row("KEY2", "bbb", "bbb2"), Row("KEY3", "ccc", "ccc2"))), structTypeUpd)
      DbCommonColumnAppender(df, "compo").writeTable("cs_test_update_ignore", SaveMode.Overwrite)

      val target = new WriteDb {
        val componentId = "cs_test_update_ignore"
        override val writeDbMode = Upsert
        override val writeDbUpdateKeys = Set("KEY")
        override val writeDbUpdateIgnoreColumns = Set("TEST", "TEST2")
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx", "11111"), Row("KEY3", "yyy", "222222"), Row("KEY4", "zzz", "33333"))), structTypeUpd)
      target.writeDb(updateDf)

      val dbCtl = new DbCtl()
      val result = dbCtl.readTable("cs_test_update_ignore").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 4
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("TEST") mustBe "aaa"
      result(0).getAs[String]("TEST2") mustBe "aaa2"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("TEST") mustBe "bbb"
      result(1).getAs[String]("TEST2") mustBe "bbb2"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("TEST") mustBe "ccc"
      result(2).getAs[String]("TEST2") mustBe "ccc2"
      result(3).getAs[String]("KEY") mustBe "KEY4"
      result(3).getAs[String]("TEST") mustBe "zzz"
      result(3).getAs[String]("TEST2") mustBe "33333"
    }

    "be normal end with commitSize" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      DbCommonColumnAppender(df, "compo").writeTable("CS_TEST", SaveMode.Overwrite)

      val target = new WriteDb {
        val componentId = "CS_TEST"
        override val writeDbMode = Upsert
        override val writeDbUpdateKeys = Set("KEY")
        override val writeDbInfo = DbConnectionInfo.bat1.copy(commitSize = Some(2))
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx"), Row("KEY3", "yyy"), Row("KEY4", "zzz"))), structType)
      target.writeDb(updateDf)

      val result = dbCtl.readTable("CS_TEST").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 4
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("TEST") mustBe "xxx"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("TEST") mustBe "bbb"
      result(1).getAs[Timestamp]("DT_D2KUPDDTTM") ne inArgs.sysSQLDate
      result(1).getAs[String]("ID_D2KUPDUSR") ne "CS_TEST"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("TEST") mustBe "yyy"
      d2s(result(2).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(2).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
      result(3).getAs[String]("KEY") mustBe "KEY4"
      result(3).getAs[String]("TEST") mustBe "zzz"
      d2s(result(3).getAs[Timestamp]("DT_D2KMKDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(3).getAs[String]("ID_D2KMKUSR") mustBe "CS_TEST"
      d2s(result(3).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(3).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
    }
  }

  "delete Logical" should {
    "be normal end" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      DbCommonColumnAppender(df, "compo").writeTable("CS_TEST", SaveMode.Overwrite)

      val target = new WriteDb {
        val componentId = "CS_TEST"
        override val writeDbMode = DeleteLogical
        override val writeDbUpdateKeys = Set("KEY")
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx"), Row("KEY3", "yyy"), Row("KEY4", "zzz"))), structType)
      target.writeDb(updateDf)

      val result = dbCtl.readTable("CS_TEST").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("TEST") mustBe "aaa"
      result(0).getAs[String]("FG_D2KDELFLG") mustBe "1"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("TEST") mustBe "bbb"
      result(1).getAs[String]("FG_D2KDELFLG") mustBe "0"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("TEST") mustBe "ccc"
      result(2).getAs[String]("FG_D2KDELFLG") mustBe "1"
      d2s(result(2).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(2).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
    }

    "delete Logical not set CommonColumns" in {
      val insert = new WriteDb {
        val componentId = "deleteLNotSetCommonColumns"
        override val writeDbMode = Insert
        override val writeDbWithCommonColumn = false
        override val writeDbSaveMode = SaveMode.Overwrite
      }

      implicit val inArgs = TestArgs().toInputArgs
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      insert.writeDb(df)

      val target = new WriteDb {
        val componentId = "deleteLNotSetCommonColumns"
        override val writeDbMode = DeleteLogical
        override val writeDbWithCommonColumn = false
        override val writeDbUpdateKeys = Set("KEY")
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx"), Row("KEY3", "yyy"))), structType)

      try {
        target.writeDb(updateDf)
        fail
      } catch {
        case t: Throwable => t.toString.contains("DeleteLogical and writeDbWithCommonColumn == false can not used be togather") mustBe true
      }
    }

    "be normal end with commitSize" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      DbCommonColumnAppender(df, "compo").writeTable("CS_TEST", SaveMode.Overwrite)

      val target = new WriteDb {
        val componentId = "CS_TEST"
        override val writeDbMode = DeleteLogical
        override val writeDbUpdateKeys = Set("KEY")
        override val writeDbInfo = DbConnectionInfo.bat1.copy(commitSize = Some(2))
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx"), Row("KEY3", "yyy"), Row("KEY4", "zzz"))), structType)
      target.writeDb(updateDf)

      val result = dbCtl.readTable("CS_TEST").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 3
      result(0).getAs[String]("KEY") mustBe "KEY1"
      result(0).getAs[String]("TEST") mustBe "aaa"
      result(0).getAs[String]("FG_D2KDELFLG") mustBe "1"
      d2s(result(0).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
      result(1).getAs[String]("KEY") mustBe "KEY2"
      result(1).getAs[String]("TEST") mustBe "bbb"
      result(1).getAs[String]("FG_D2KDELFLG") mustBe "0"
      result(2).getAs[String]("KEY") mustBe "KEY3"
      result(2).getAs[String]("TEST") mustBe "ccc"
      result(2).getAs[String]("FG_D2KDELFLG") mustBe "1"
      d2s(result(2).getAs[Timestamp]("DT_D2KUPDDTTM")) mustBe d2s(inArgs.sysSQLDate)
      result(2).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
    }
  }

  "delete Physical" should {
    "be normal end" in {
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      DbCommonColumnAppender(df, "compo").writeTable("CS_TEST", SaveMode.Overwrite)

      val target = new WriteDb {
        val componentId = "CS_TEST"
        override val writeDbMode = DeletePhysical
        override val writeDbUpdateKeys = Set("KEY")
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx"), Row("KEY3", "yyy"), Row("KEY4", "zzz"))), structType)
      target.writeDb(updateDf)

      val result = dbCtl.readTable("CS_TEST").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 1
      result(0).getAs[String]("KEY") mustBe "KEY2"
      result(0).getAs[String]("TEST") mustBe "bbb"
    }

    "delete Physical not set CommonColumns" in {
      val insert = new WriteDb {
        val componentId = "delPNotSetCommonColumns"
        override val writeDbMode = Insert
        override val writeDbWithCommonColumn = false
        override val writeDbSaveMode = SaveMode.Overwrite
      }

      implicit val inArgs = TestArgs().toInputArgs
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "aaa"), Row("KEY2", "bbb"), Row("KEY3", "ccc"))), structType)
      insert.writeDb(df)

      val target = new WriteDb {
        val componentId = "delPNotSetCommonColumns"
        override val writeDbMode = DeletePhysical
        override val writeDbWithCommonColumn = false
        override val writeDbUpdateKeys = Set("KEY")
      }
      val updateDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx"), Row("KEY3", "yyy"))), structType)
      target.writeDb(updateDf)

      val dbCtl = new DbCtl()
      val result = dbCtl.readTable("delPNotSetCommonColumns").collect.toList.sortBy(_.getAs[String]("KEY"))
      result.length mustBe 1
      result(0).getAs[String]("KEY") mustBe "KEY2"
      result(0).getAs[String]("TEST") mustBe "bbb"
    }
  }

  "DbCommonColumnAppender" should {
    "be normal end" in {
      implicit val inArgs = TestArgs().toInputArgs
      val target = new WriteDb {
        val componentId = "CS_TEST"
        override val writeDbSaveMode = SaveMode.Overwrite
      }
      val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
        Row("KEY1", "xxx"))), structType)
      target.writeDb(df)

      val result = dbCtl.readTable("CS_TEST").collect.toList.sortBy(_.getAs[String]("KEY"))
      result(0).getAs[Timestamp]("DT_D2KMKDTTM").toString.isEmpty mustBe false
      result(0).getAs[String]("ID_D2KMKUSR") mustBe "CS_TEST"
      result(0).getAs[Timestamp]("DT_D2KUPDDTTM").toString.isEmpty mustBe false
      result(0).getAs[String]("ID_D2KUPDUSR") mustBe "CS_TEST"
      result(0).getAs[String]("NM_D2KUPDTMS") mustBe "0"
      result(0).getAs[String]("FG_D2KDELFLG") mustBe "0"
    }
  }

  "writeDbConvNaMode" should {
    val ts = Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 0, 0, 0))
    val df = Seq(
      Sp01(ts, 10, 100, ts, "xx", "yy"),
      Sp01(null, null, null, null, null, null),
      Sp01(null, null, null, null, "", null),
      Sp01(null, null, null, null, null, "")).toDF.repartition(1)

    "trueの場合 Stringのnull及び空文字をspace1文字に変換する" in {
      val target = new WriteDb {

        override val writeDbConvNaMode: Boolean = true

        val componentId = "sp01"
        override val writeDbMode = Insert
        override val writeDbSaveMode: SaveMode = SaveMode.Overwrite
        override val writeDbWithCommonColumn: Boolean = false
      }

      target.writeDb(df)

      val result = dbCtl.readTable("sp01").as[Sp01].collect
      result(0) mustBe Sp01(ts, 10, 100.00, ts, "xx", "yy   ")
      result(1) mustBe Sp01(null, null, null, null, " ", "     ")
      result(2) mustBe Sp01(null, null, null, null, " ", "     ")
      result(3) mustBe Sp01(null, null, null, null, " ", "     ")
    }

    "falseの場合変換しない" in {
      val target = new WriteDb {

        override val writeDbConvNaMode: Boolean = false

        val componentId = "sp01"
        override val writeDbMode = Insert
        override val writeDbSaveMode: SaveMode = SaveMode.Overwrite
        override val writeDbWithCommonColumn: Boolean = false
      }

      target.writeDb(df)

      val result = dbCtl.readTable("sp01").as[Sp01].collect
      result(0) mustBe Sp01(ts, 10, 100.00, ts, "xx", "yy   ")
      result(1) mustBe Sp01(null, null, null, null, null, null)
      result(2) mustBe Sp01(null, null, null, null, null, null)
      result(3) mustBe Sp01(null, null, null, null, null, null)
    }
  }
}
