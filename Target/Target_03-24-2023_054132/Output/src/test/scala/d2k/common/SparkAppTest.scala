package d2k.common

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts
import spark.common.DfCtl._
import spark.common.DfCtl.implicits._
import com.snowflake.snowpark.DataFrame
import d2k.common.df.Executor
import d2k.common.df.template.DfToDf
import d2k.common.df.executor._
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object SparkAppTestObj extends SparkApp {
case class Aaa (a: String, b: String)
import SparkContexts.context.implicits._
 def exec(implicit inArgs: InputArgs) = {
   val df = Seq(Aaa("a", "b")).toDF
comp1.run(df)
   }
 val comp1 = new DfToDf with Executor {
      val componentId = "MCA018121"

      def invoke(df: DataFrame)(implicit inArgs: InputArgs) = df ~> f01

      def f01 = (_ : DataFrame).filter($"a" === "a")
   }
}
 class SparkAppTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "SparkApp" should {
   "be closing session" in {
      SparkAppTestObj.main(TestArgs().toArray)
      }
   }
}