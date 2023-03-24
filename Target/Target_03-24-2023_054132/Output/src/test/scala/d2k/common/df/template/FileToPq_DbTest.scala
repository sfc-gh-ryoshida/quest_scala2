package d2k.common.df.template

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.common.TestArgs
import com.snowflake.snowpark.types._
import spark.common.DbCtl
import spark.common.SparkContexts._
import com.snowflake.snowpark.Row
import spark.common.PqCtl
import d2k.common.df.CsvInfo
import d2k.common.df.executor.PqCommonColumnRemover
import com.snowflake.snowpark.SaveMode
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class FileToPq_DbTest extends WordSpec with MustMatchers with BeforeAndAfter {
   implicit val inArgs = TestArgs().toInputArgs

   val structType = StructType(Seq(
StructField("KEY", StringType), StructField("TEST", StringType)))

   val dbCtl = new DbCtl ()

   import dbCtl.implicits._

   val pqCtl = new PqCtl (inArgs.baseOutputFilePath)

   "FileToPq_Db" should {
   "be success fileToDb" in {
      val target = new FileToDb with PqCommonColumnRemover {
            val componentId = "cs_xxx"

            val fileInputInfo = CsvInfo(Set("cs_xxx1.dat"))

            override lazy val writeTableName = "cs_test"

            override val writeDbSaveMode = SaveMode.Overwrite
         }
 val insertDf = context.createDataFrame(sc.makeRDD(Seq(
Row("key1", "aaa"))), structType)
target.run(Unit)
 val result = dbCtl.readTable("cs_test").collect.toList.sortBy(_.getAs[String]("KEY"))
result.length mustBe 2
result(0).getAs[String]("TEST") mustBe "aaa"
result(1).getAs[String]("TEST") mustBe "bbb"
result(0).getAs[String]("ID_D2KUPDUSR") mustBe "cs_xxx"
      }
"be same result" in {
      val target = new FileToPq_Db with PqCommonColumnRemover {
            val componentId = "cs_xxx"

            val fileInputInfo = CsvInfo(Set("cs_xxx2.dat"))

            override lazy val writeTableName = "cs_test"

            override val writeDbSaveMode = SaveMode.Overwrite
         }
 val insertDf = context.createDataFrame(sc.makeRDD(Seq(
Row("key1", "aaa"))), structType)
target.run(Unit)
 val result = dbCtl.readTable("cs_test").collect.toList.sortBy(_.getAs[String]("KEY"))
result.length mustBe 2
result(0).getAs[String]("TEST") mustBe "ccc"
result(1).getAs[String]("TEST") mustBe "ddd"
result(0).getAs[String]("ID_D2KUPDUSR") mustBe "cs_xxx"
 val resultPq = pqCtl.readParquet("cs_test").collect.toList.sortBy(_.getAs[String]("KEY"))
result.length mustBe 2
resultPq(0).getAs[String]("TEST") mustBe "ccc"
result(1).getAs[String]("TEST") mustBe "ddd"
result(0).getAs[String]("ID_D2KUPDUSR") mustBe "cs_xxx"
      }
   }
}