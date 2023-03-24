package d2k.common.df.template

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import d2k.common.df._
import d2k.common.InputArgs
import d2k.common.SparkApp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import spark.common.PqCtl
import scala.io.Source
import spark.common.FileCtl
import org.apache.spark.sql._
import spark.common.SparkContexts.context
import org.apache.spark.sql.types._
import spark.common.SparkContexts
import d2k.common.df.executor.Nothing
import d2k.common.TestArgs

class DfToFileTest extends WordSpec with MustMatchers with BeforeAndAfter {
  val outConfPath = "data/test/conv/conf/mba_eo.conf"
  val applicationId = "APP-ID-001"
  implicit val inArgs = TestArgs().toInputArgs.copy(processId = "procId", applicationId = applicationId,
    runningDateFileFullPath = "data/test/conv/conf/COM_DATEFILE_SK0.txt", fileConvOutputFile = outConfPath)
  val outputFilePath = s"C:/tmp/output/${applicationId}.dat"

  object Test1 extends SparkApp {

    val structType = StructType(Seq(
      StructField("COL1", StringType),
      StructField("COL2", StringType),
      StructField("COL3", StringType)))

    val df = context.createDataFrame(SparkContexts.sc.makeRDD(Seq(Row("AAA", "01", "一"))), structType)
    def targetComponent = new DfToFile with Nothing {
      val componentId = applicationId
      override val writeFileFunc = writeFixedFunc
    }
    def exec(implicit inArgs: InputArgs) = targetComponent.run(df)(inArgs)
  }

  val writeFixedFunc = (df: DataFrame, inArgs: InputArgs, conf: Map[String, String]) => {
    def rpad(target: String, len: Int, pad: String = " ") = {
      val str = if (target == null) { "" } else { target }
      val strSize = str.getBytes("MS932").size
      val padSize = len - strSize
      s"${str}${pad * padSize}"
    }

    val outputDir = conf(s"${inArgs.applicationId}.outputDir")
    val outputFile = conf(s"${inArgs.applicationId}.outputFile")

    val itemLens = conf(s"${inArgs.applicationId}.itemLengths")
    val itemLenList = itemLens.split(',').map { len => len.toInt }

    val itemLenListWithIdx = itemLenList.zipWithIndex
    def rowToFixedStr(row: Row) = {
      val line = itemLenListWithIdx.foldLeft("") { (acum, elem) =>
        {
          val (len, idx) = elem
          val str = row.getString(idx)
          acum + rpad(str, len)
        }
      }
      line + "\n"
    }
    FileCtl.writeToFile(s"${outputDir}/${outputFile}") { writer => df.rdd.map(rowToFixedStr).collect.foreach(writer.print) }
  }
}
