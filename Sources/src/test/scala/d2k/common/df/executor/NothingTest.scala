package d2k.common.df.executor

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import spark.common.SparkContexts
import d2k.common.SparkApp
import d2k.common.InputArgs
import d2k.common.df.Executor
import org.apache.spark.sql.DataFrame

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
