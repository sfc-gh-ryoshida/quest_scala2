package d2k.common.df.template

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts.context
import org.apache.spark.sql.Row
import d2k.common.df.executor
import d2k.common.df.template
import d2k.common.TestArgs

class DfUnionToDfTest extends WordSpec with MustMatchers with BeforeAndAfter {
  case class Test(a: String)

  "DfUnionToDfTest" should {
    "normal end" in {
      implicit val inArgs = TestArgs().toInputArgs
      val comp = new template.DfUnionToDf with executor.Nothing {
        val componentId = "test"
      }
      val df1 = context.createDataFrame(Seq(Test("aaa")))
      val df2 = context.createDataFrame(Seq(Test("bbb")))
      val result = comp.run(df1, df2).collect
      result(0).getAs[String]("a") mustBe "aaa"
      result(1).getAs[String]("a") mustBe "bbb"
    }
  }
}