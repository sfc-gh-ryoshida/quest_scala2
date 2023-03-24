package d2k.common.df.executor

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.Row
import spark.common.SparkContexts
import d2k.common.SparkApp
import d2k.common.InputArgs
import d2k.common.df.Executor
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class NothingTest extends WordSpec with MustMatchers with BeforeAndAfter {
   val structType = StructType(Seq(
StructField("dummy", StringType)))

   val df = SparkContexts.context.createDataFrame(SparkContexts.sc.makeRDD(Seq(
Row("aa"))), structType)

   implicit val inArgs = InputArgs("", "", "", "data/test/conv/conf/COM_DATEFILE_SK0.txt")

   "NothingTest" should {
   "normal end" in {
      val result = df.collect
result(0).length mustBe 1
      }
   }
}