package d2k.common.df.mixIn

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import d2k.common.df.template._
import d2k.common.df.executor._
import d2k.app.test.common.TestArgs
import scala.io.Source
import reflect.io._
import Path._
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class OraLoaderHdfsTest extends WordSpec with MustMatchers with BeforeAndAfter {
   import spark.common.SparkContexts.context.implicits._

   implicit val inArgs = TestArgs().toInputArgs

   val compo1 = new DfToFile with OraLoaderHdfs with Nothing {
      val componentId = "test_hdfs"
   }

   "OraLoader" should {
   "normal end" in {
      val df = Seq(OraLoaderTestData("""ｶﾅ"1""", "ｶﾅ2")).toDF
compo1.run(df)
 val filePath = s"${ sys.env("DB_LOADING_FILE_PATH") }/${ compo1.componentId }"
 val fileName = filePath.toDirectory.files.map(_.name).filter(_.endsWith(".csv")).toSeq.head
Source.fromFile(s"${ filePath }/${ fileName }").getLines.foreach{ line =>line mustBe """"ｶﾅ""1","ｶﾅ2""""
}
      }
   }
}