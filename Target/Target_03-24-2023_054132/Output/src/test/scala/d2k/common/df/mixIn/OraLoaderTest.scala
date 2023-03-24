package d2k.common.df.mixIn

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import d2k.common.df.template._
import d2k.common.df.executor._
import d2k.app.test.common.TestArgs
import scala.io.Source
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class OraLoaderTestData (s1: String, s2: String)
 class OraLoaderTest extends WordSpec with MustMatchers with BeforeAndAfter {
   import spark.common.SparkContexts.context.implicits._

   implicit val inArgs = TestArgs().toInputArgs

   val compo1 = new DfToFile with OraLoader with Nothing {
      val componentId = "test"
   }

   "OraLoader" should {
   "normal end" in {
      val df = Seq(OraLoaderTestData("""ｶﾅ"1""", "ｶﾅ2")).toDF
compo1.run(df)
 val filePath = s"${ sys.env("DB_LOADING_FILE_PATH") }/test"
Source.fromFile(filePath)("MS932").getLines.foreach{ line =>line mustBe """"ｶﾅ""1","ｶﾅ2""""
}
      }
   }
}