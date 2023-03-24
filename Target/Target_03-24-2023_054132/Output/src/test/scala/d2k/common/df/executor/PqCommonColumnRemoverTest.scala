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
import spark.common.SparkContexts.context
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class PqCommonColumnRemoverTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "PqCommonColumnRemover test" in {
   implicit val inArgs = InputArgs("", "", "", "data/test/conv/conf/COM_DATEFILE_SK0.txt")
 val structType = StructType(Seq(StructField("DUMMY1", StringType), StructField("DUMMY2", StringType), StructField("ROW_ERR", StringType), StructField("ROW_ERR_MESSAGE", StringType)))
 val beforeDf = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(Row("dummy1", "dummy2", "false", "dummy error message"))), structType)
 val beforeArr = beforeDf.collect
 val afterDf = PqCommonColumnRemover.apply(beforeDf)(inArgs)
 val afterArr = afterDf.collect
beforeArr(0).length mustBe 4
afterArr(0).length mustBe 2
afterArr(0).getAs[String]("DUMMY1") mustBe "dummy1"
afterArr(0).getAs[String]("DUMMY2") mustBe "dummy2"
   }
}