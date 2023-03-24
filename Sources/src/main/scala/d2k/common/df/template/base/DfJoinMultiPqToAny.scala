package d2k.common.df.template.base

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column

import spark.common.PqCtl
import d2k.common.InputArgs
import d2k.common.df.flow.OneInToOneOutForDf
import scala.util.Try

case class JoinPqInfo(name: String, joinExprs: Column, dropCols: Set[String] = Set.empty[String], prefixName: String = "", joinType: String = "left_outer")

trait DfJoinMultiPqToAny[OUT] extends OneInToOneOutForDf[DataFrame, OUT] {
  val prefixName: String
  val joinPqInfoList: Seq[JoinPqInfo]
  lazy val envName: String = ""
  private[this] val inputFilePath = Try { sys.env(s"PQ_INPUT_PATH_${envName}") }

  def preExec(left: DataFrame)(implicit inArgs: InputArgs): DataFrame = {
    val inFilePath = inputFilePath.getOrElse(inArgs.baseInputFilePath)
    val orgDf = left.columns.foldLeft(left)((df, name) => df.withColumnRenamed(name, s"$prefixName#$name"))
    joinPqInfoList.foldLeft(orgDf) { (odf, pqInfo) =>
      val pqDf = new PqCtl(inFilePath).readParquet(pqInfo.name)
      val pname = if (pqInfo.prefixName.isEmpty) pqInfo.name else pqInfo.prefixName
      val addNameDf = pqDf.columns.foldLeft(pqDf) { (df, name) =>
        df.withColumnRenamed(name, s"$pname#$name")
      }
      val joinedDf = odf.join(addNameDf, pqInfo.joinExprs, pqInfo.joinType)
      pqInfo.dropCols.foldLeft(joinedDf)((l, r) => l.drop(r))
    }
  }
}
