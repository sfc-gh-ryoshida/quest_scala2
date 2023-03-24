package d2k.common.df.template.base

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import d2k.common.InputArgs
import d2k.common.df.flow.TwoInToOneOutForDf
import d2k.common.df.SingleReadPq

trait TwoPqJoinToAny[OUT] extends TwoInToOneOutForDf[Unit, Unit, OUT] with SingleReadPq {
  val leftPqName: String
  val rightPqName: String
  val joinType = "left_outer"
  def joinExprs(left: DataFrame, right: DataFrame): Column
  def select(left: DataFrame, right: DataFrame): Seq[Column]

  final def preExec(in1: Unit, in2: Unit)(implicit inArgs: InputArgs): DataFrame = {
    val left = readParquetSingle(leftPqName)
    val right = readParquetSingle(rightPqName)
    val joined = left.join(right, joinExprs(left, right), joinType)
    joined.select(select(left, right).toArray: _*)
  }
}
