package spark.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import scala.util.matching.Regex

object DfCtl {
  sealed trait Editors {
    val colName: String
    def toCols(colNames: Set[String], cols: Seq[Column]): (Set[String], Seq[Column])
  }

  case class Edit(inColName: String, editor: Column) extends Editors {
    val colName = inColName
    def toCols(colNames: Set[String], cols: Seq[Column]) =
      (colNames - colName, cols :+ (editor as colName))
  }

  case class Rename(colNameFrom: String, colNameTo: String, editor: Column = null) extends Editors {
    val colName = s"${colNameFrom}_${colNameTo}"
    def toCols(colNames: Set[String], cols: Seq[Column]) =
      Option(editor).map(e => (colNames - colNameFrom, cols :+ (editor as colNameTo)))
        .getOrElse((colNames - colNameFrom, cols :+ (col(colNameFrom) as colNameTo)))
  }

  case class Delete(inColName: String) extends Editors {
    val colName = inColName
    def toCols(colNames: Set[String], cols: Seq[Column]) =
      (colNames - colName, cols)
  }

  case class CastByName(inColName: String, castType: String) extends Editors {
    val colName = inColName
    def toCols(colNames: Set[String], cols: Seq[Column]) =
      (colNames - colName, cols :+ (col(inColName).cast(castType)))
  }

  case class CastByRegex(inRegex: String, castType: String) extends Editors {
    val colName = inRegex
    def toCols(colNames: Set[String], cols: Seq[Column]) = {
      val regxMatchNames = colNames.flatMap(inRegex.r.findFirstIn)
      (colNames -- regxMatchNames, cols ++ regxMatchNames.map(name => col(name).cast(castType)))
    }
  }

  case class ApplyByName(inColName: String, func: Column => Column) extends Editors {
    val colName = inColName
    def toCols(colNames: Set[String], cols: Seq[Column]) =
      (colNames - colName, cols :+ func(col(inColName)))
  }

  case class ApplyByRegex(inRegex: String, func: Column => Column) extends Editors {
    val colName = inRegex
    def toCols(colNames: Set[String], cols: Seq[Column]) = {
      val regxMatchNames = colNames.flatMap(inRegex.r.findFirstIn)
      (colNames -- regxMatchNames, cols ++ regxMatchNames.map(name => func(col(name))))
    }
  }

  def editColumns(editors: Seq[Editors]) = (df: DataFrame) => {
    val (colNames, cols) = editors.foldLeft((df.schema.fieldNames.toSet, Seq.empty[Column])) {
      case ((colNames, cols), target) => target.toCols(colNames, cols)
    }
    val schemaNames = colNames.map(d => col(d))
    df.select((schemaNames.toSeq ++ cols): _*)
  }

  def editColumnsAndSelect(editors: Seq[Editors]) = (df: DataFrame) => {
    val (colNames, cols) = editors.foldLeft((df.schema.fieldNames.toSet, Seq.empty[Column])) {
      case ((colNames, cols), target) => target.toCols(colNames, cols)
    }
    df.select(cols: _*)
  }

  def applyAll(colNames: Seq[String], applyCode: Column => Column) = (df: DataFrame) => {
    val schemaNames = colNames.map(d => applyCode(col(d)))
    df.select(schemaNames: _*)
  }

  def selectMaxValue(targetCols: Seq[String], orderCols: Seq[Column]) = (df: DataFrame) => {
    val win = Window.partitionBy(targetCols.map(col): _*).orderBy(orderCols: _*)
    df.withColumn("rank", row_number.over(win)).filter("rank = 1").drop("rank")
  }

  def groupingAgg(groupingColumns: Seq[String], aggColumns: Seq[Column]) = (df: DataFrame) => {
    df.groupBy(groupingColumns.map(col): _*).agg(aggColumns.head, aggColumns.tail: _*)
  }

  def groupingSum(groupingColumns: Seq[String], sumColumns: Seq[String]) = (df: DataFrame) => {
    val cols = sumColumns.map(col => sum(col) as col)
    groupingAgg(groupingColumns, cols)(df)
  }

  def addColumnPrefix(name: String) = (df: DataFrame) => {
    val cols = df.schema.map(x => df(x.name) as s"${name}_${x.name}")
    df.select(cols: _*)
  }

  def dropColumnPrefix(name: String) = (df: DataFrame) => {
    val cols = df.schema.map(_.name).filter(!_.startsWith(s"${name}_")).map(col)
    df.select(cols: _*)
  }

  object implicits {
    implicit class MyCommonDF(df: DataFrame) extends Logging {
      def ~>(f: DataFrame => DataFrame) = f(df)
      def ~|>(f: DataFrame => DataFrame) = { df.show(false); f(df) }

      def pickMaxValueRow(pks: String*)(maxValueTarget: String*) =
        df.sort((pks.map(x => col(x)) ++ maxValueTarget.map(x => col(x).desc)): _*).dropDuplicates(pks)

      def partitionWriteFile(
        filePath: String, overwrite: Boolean = true, charEnc: String = "MS932", partitionExtention: String = "")(
        func: Row => String) {
        if (overwrite) FileCtl.deleteDirectory(filePath)
        FileCtl.createDirectory(filePath)

        df.rdd.mapPartitionsWithIndex { (idx, iterRow) =>
          val fullPath = FileCtl.addExtention(s"${filePath}/${idx}", partitionExtention)
          FileCtl.writeToFile(fullPath, true, charEnc) { pw =>
            elapse(s"fileWrite:${fullPath}") {
              iterRow.foreach(row => pw.println(func(row)))
            }
          }
          Seq.empty[Row].toIterator
        }.foreach(_ => ())
      }

      def partitionWriteToFileWithPartitionColumns(
        filePath: String, partitionColumns: Seq[String],
        overwrite: Boolean = true, charEnc: String = "MS932", partitionExtention: String = "")(
        func: Row => String) {
        if (overwrite) FileCtl.deleteDirectory(filePath)
        FileCtl.createDirectory(filePath)

        df.rdd.mapPartitionsWithIndex { (idx, iterRow) =>
          val fullPath = FileCtl.addExtention(s"${filePath}/${idx}", partitionExtention)
          elapse(s"fileWrite:${fullPath}") {
            FileCtl.loanPrintWriterCache { cache =>
              iterRow.foldLeft(cache) { (l, r) =>
                FileCtl.writeToFileWithPartitionColumns(
                  filePath, idx, charEnc, partitionColumns, partitionExtention)(func)(l)(r)
              }
            }
          }
          Seq.empty[Row].toIterator
        }.foreach(_ => ())
      }
    }

    implicit class MyCommonDFTouple(df: (DataFrame, DataFrame)) extends Logging {
      def ~>(f: (DataFrame, DataFrame) => DataFrame) = f(df._1, df._2)
      def ~|>(f: (DataFrame, DataFrame) => DataFrame) = { df._1.show(false); df._2.show(false); f(df._1, df._2) }
    }

    implicit class ToEdit(x: (String, Column)) {
      def e = Edit(x._1, x._2)
    }

    implicit class ToEditSeq(x: Seq[(String, Column)]) {
      def e = x.map(_.e)
    }

    implicit class ToToupleLogic(x: (String, String)) {
      def r = Rename(x._1, x._2)
      def c = CastByName(x._1, x._2)
      def cr = CastByRegex(x._1, x._2)
    }

    implicit class ToToupleLogicSeq1(x: Seq[(String, String)]) {
      def r = x.map(_.r)
      def c = x.map(_.c)
      def cr = x.map(_.cr)
    }

    implicit class ToToupleFunctionLogic(x: (String, Column => Column)) {
      def a = ApplyByName(x._1, x._2)
      def ar = ApplyByRegex(x._1, x._2)
    }

    implicit class ToToupleFunctionLogicSeq1(x: Seq[(String, Column => Column)]) {
      def a = x.map(_.a)
      def ar = x.map(_.ar)
    }

    implicit class ToRename2(x: ((String, String), Column)) {
      def r = Rename(x._1._1, x._1._2, x._2)
    }

    implicit class ToRenameSeq2(x: Seq[((String, String), Column)]) {
      def r = x.map(_.r)
    }

    implicit class ToDelete(x: String) {
      def d = Delete(x)
    }

    implicit class ToDeleteSeq(x: Seq[String]) {
      def d = x.map(_.d)
    }
  }
}
