package d2k.common.df.template.base

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.common.df.executor
import d2k.common.df.template
import com.snowflake.snowpark.functions._
import spark.common.SparkContexts.context.implicits._
import spark.common.DfCtl._
import implicits._
import com.snowflake.snowpark.DataFrame
import d2k.common.TestArgs
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class TwoDfJoinToAnyTest extends WordSpec with MustMatchers with BeforeAndAfter {
   implicit val inArgs = TestArgs().toInputArgs

   case class TestA (key: String = "key", a: String = "a1")

   case class TestB (key: String = "key", a: String = "a2", b: String = "b2")

   "mergeDropDuplicate" should {
   val dfA = Seq(TestA()).toDF
 val dfB = Seq(TestB()).toDF
"be success" in {
      val comp = new template.DfJoinToDf with executor.Nothing {
            val componentId = "test"

            def joinExprs(left: DataFrame, right: DataFrame) = left("key") === right("key")

            def select(left: DataFrame, right: DataFrame) = mergeDropDuplicate(left, right)
         }
 val result = comp.run(dfA, dfB).collect
result.foreach{ row =>row.schema.size mustBe 3
row.getAs[String]("key") mustBe "key"
row.getAs[String]("a") mustBe "a1"
row.getAs[String]("b") mustBe "b2"
}
      }
   }

   "mergeWithName" should {
   val dfA = Seq(TestA()).toDF
 val dfB = Seq(TestB()).toDF
"be success" in {
      val comp = new template.DfJoinToDf with executor.Nothing {
            val componentId = "test"

            def joinExprs(left: DataFrame, right: DataFrame) = left("key") === right("key")

            def select(left: DataFrame, right: DataFrame) = mergeWithPrefix(left, right, "XXX")
         }
 val result = comp.run(dfA, dfB).collect
result.foreach{ row =>row.schema.size mustBe 5
row.getAs[String]("key") mustBe "key"
row.getAs[String]("a") mustBe "a1"
row.getAs[String]("XXX_key") mustBe "key"
row.getAs[String]("XXX_a") mustBe "a2"
row.getAs[String]("XXX_b") mustBe "b2"
}
      }
   }
}