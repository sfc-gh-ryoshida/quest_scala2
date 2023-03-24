/*
EWI: SPRKSCL1001 => This code section has parsing errors, so it was commented out
package d2k.appdefdoc.gen.src

import d2k.appdefdoc.parser._
import org.apache.spark.sql.catalyst.expressions.StartsWith

object ConvertTemplateDefine {
  val fileInfoClass = Map(
    ("csv" -> "CsvInfo"),
    ("tsv" -> "TsvInfo"),
    ("vsv" -> "VsvInfo"),
    ("ssv" -> "SsvInfo"),
    ("fixed" -> "FixedInfo"))

  val fileInputNameConverter = Map(
    ("読込ファイル形式" -> "fileInputInfo"),
    ("読込ファイル名" -> "inputFiles"),
    ("読込ファイルパス設定名" -> "envName"),
    ("ダブルクォーテーション削除(only TsvInfo)" -> "dropDoubleQuoteMode"),
    ("ヘッダ有無" -> "header"),
    ("フッタ有無" -> "footer"),
    ("改行有無" -> "newLine"),
    ("連番指定有無(only FixedInfo)" -> "withIndex"),
    ("レコード長チェック有無(only FixedInfo)" -> "recordLengthCheck"),
    ("文字コード" -> "charSet"),
    ("改行コード" -> "newLineCode"),
    ("ドメインコンバート事前Filter(only FixedInfo)" -> "preFilter"))

  def createFileInputInfo(d: ProcessDetail) = {
    val fileInfo = d.values.flatMap {
      case ProcessParamSubDetail(id, name, defcode, value) if !value.startsWith("実装不要") =>
        Some((fileInputNameConverter(name.trim), value.mkString.trim))
      case _ => None
    }.toMap
    val fileInfoStr = (fileInfo - "fileInputInfo").map { case (n, v) => s"${n} = ${v}" }.mkString(", ")
    s"${fileInfoClass(fileInfo("fileInputInfo").toLowerCase)}(${fileInfoStr})"
  }

  def createFileOutputMode(baseUrl: String, branch: String)(cd: ComponentDetail) = {
    val fileOutput = cd.processes.find(_.desc.value.startsWith("File出力"))
    val fileKind = fileOutput.map(_.detail.filter(_.param.name.startsWith("出力ファイル種別")))
    val fileKindDef = fileKind.map { x =>
      val a = x.head
      (a.defaultCode.get.value.split('=').head.trim, a.values.map {
        case ProcessParamValue(v) => v
        case _                    => ""
      }.head)
    }
    val fileParam = fileOutput.map(_.detail.filter(_.param.name.startsWith("出力ファイルパラメータ")))
    val fileParamDetails = fileParam.map { x =>
      x.flatMap(_.values.flatMap {
        case ProcessParamSubDetail(_, name, default, value) => Some((name, value))
        case _ => None
      })
    }

    def paramDetailValue(name: String) = fileParamDetails.flatMap(_.find(_._1.startsWith(name)).map(_._2.mkString).flatMap {
      case "実装不要" => None
      case x      => Some(x)
    })

    val fileParamOutputLen = paramDetailValue("出力項目長").map { v =>
      if (v.contains("項目定義物理名")) {
        val tableData = v.split('|').toList.takeRight(2)
        val item = tableData.head.split("\\]\\(")
        val resId = item(0).tail
        val filePath = item(1).trim.dropRight(1).split("/").toList.takeRight(3).mkString("/")
        println(s"  Parsing ${resId.drop(1)}[${tableData.last.trim}]")
        val itemPs = ItemDefParser(baseUrl, branch, filePath).get
        val lastItem = itemPs.details.last
        itemPs.details.dropRight(1).map { d =>
          s"        ${d.size}, //${d.name}"
        }.mkString("\n", "\n", "\n") + s"        ${lastItem.size} //${lastItem.name}\n      "
      } else {
        v
      }
    }
    val fileParamDqCol = paramDetailValue("ダブルクォート対象カラム名")
    fileKindDef.flatMap { x =>
      x._2.trim.toLowerCase match {
        case "csv" => Some(fileParamDqCol.map { c =>
          val splitted = c.mkString.split(',')
          s"WriteFileMode.Csv(${splitted.mkString("\"", s"""", """", "\"")})"
        }.getOrElse("WriteFileMode.Csv"))
        case "tsv"            => Some("WriteFileMode.Tsv")
        case "fixed" | "実装不要" => Some(fileParamOutputLen.map { c => s"WriteFileMode.Fixed(${c})" }.getOrElse("WriteFileMode.Fixed"))
        case _                => None
      }
    }.getOrElse("実装不要")
  }

  def convertScalaSource(baseUrl: String, branch: String)(cd: ComponentDetail, d: ProcessDetail) = {
    d.defaultCode.flatMap { dc =>
      val seqData = d.values.flatMap {
        case ProcessParamValue(value) if d.param.name == "空DataFrame用Schema" => {
          val regx = """.*\((.*\.md)\)""".r
          Some(regx.findFirstMatchIn(value).map { r =>
            val path = r.group(1).split("/").toList.takeRight(3).mkString("/")
            val itemPs = ItemDefParser(baseUrl, branch, path)
            itemPs.get.details.map { x =>
              s"""("${x.id}", "${x.dataType}")"""
            }.mkString("Seq(", ",", ")")
          }.getOrElse(value))
        }
        case ProcessParamValue(value) if d.param.name == "結合条件" =>
          Some(ItemReplace.column.replaceLitStr(ItemReplace.column.replaceJoinStr(value)))
        case ProcessParamValue(value) if d.param.name.startsWith("DB読込時条件") =>
          Some((ItemReplace.scala.replaceLitStr _ andThen ItemReplace.db.replaceLitStringMod)(value))
        case ProcessParamValue(value) =>
          Some((ItemReplace.scala.replaceLitStr _ andThen ItemReplace.scala.replaceLitStringMod)(value))
        case x => None
      }
      val data = (d.param.name match {
        case "空DataFrame用Schema" => seqData.filterNot(_.startsWith("|"))
        case "読込ファイル情報"          => Seq(createFileInputInfo(d))
        case "出力ファイル種別"          => Seq(createFileOutputMode(baseUrl, branch)(cd))
        case _                   => seqData
      }).mkString("      ", "\n      ", "")

      if (data.contains("実装不要")) {
        None
      } else {
        val overrideVal = if (dc.value.contains(" = ")) "override" else ""
        Some(s"\n    ${overrideVal} ${dc.value.split(" = ").head} =\n${data}")
      }
    }
  }

  def replaceJoinSelect(targetData: String, defaultCode: String)(
    splitPattern: String, dropSize: Int,
    replaceLeft: String => String, replaceRight: String => String) = {
    val splitted = targetData.split(splitPattern)
    for {
      left <- splitted.headOption.map(_.drop(dropSize))
      right <- splitted.tail.headOption
    } yield {
      val replacedLeft = replaceLeft(left)
      val replacedRight = replaceRight(right)
      s"\n    ${defaultCode} = Seq(\n    ${replacedLeft}\n    ${replacedRight}\n    )\n"
    }
  }

  def replacePrefix(defaultCode: String, prefixName: String) =
    s"""\n    ${defaultCode} = \n      mergeWithPrefix(left, right, "${prefixName}")\n"""

  def replaceDropDuplicate(defaultCode: String) =
    s"\n    ${defaultCode} = \n      mergeDropDuplicate(left, right)\n"

  def replaceLeftRight(baseUrl: String, branch: String, targetData: String)(cd: ComponentDetail, d: ProcessDetail) = {
    val hasLeftDrop = targetData.contains("left.drop")
    val hasRightDrop = targetData.contains("right.drop")
    val replacer = replaceJoinSelect(targetData, d.defaultCode.get.value) _
    val replaced = (hasLeftDrop, hasRightDrop) match {
      case (true, true) =>
        replacer("right.drop:", "left.drop:".size,
          ItemReplace.join.genJoinSelectorDrop(_, false).mkString("  left", "\n          ", ""),
          ItemReplace.join.genJoinSelectorDrop(_, true).mkString("  right", "\n           ", ""))
      case (true, false) =>
        replacer("right:", "left.drop:".size,
          ItemReplace.join.genJoinSelectorDrop(_, false).mkString("  left", "\n          ", ""),
          ItemReplace.join.genJoinSelectorAppend(_, true).mkString("  right", "\n      right", ""))
      case (false, true) =>
        replacer("right.drop:", "left.drop:".size,
          ItemReplace.join.genJoinSelectorAppend(_, false).mkString("  left", "\n      left", ""),
          ItemReplace.join.genJoinSelectorDrop(_, true).mkString("  right", "\n           ", ""))
      case _ =>
        replacer("right:", "left:".size,
          ItemReplace.join.genJoinSelectorAppend(_, false).mkString("  left", "\n      left", ""),
          ItemReplace.join.genJoinSelectorAppend(_, true).mkString("  right", "\n      right", ""))
    }
    replaced.orElse(convertScalaSource(baseUrl, branch)(cd, d))
  }

  def modDetail(baseUrl: String, branch: String)(cd: ComponentDetail, d: ProcessDetail) = {
    val targetData = d.values.map {
      case v: ProcessParamValue => v.value.replaceAllLiterally(" ", "")
      case v                    => v
    }.mkString

    (d.param.id, d.param.name) match {
      case ("04", "項目選択") if targetData.startsWith("left") =>
        replaceLeftRight(baseUrl, branch, targetData)(cd, d)
      case ("04", "項目選択") if targetData.startsWith("接頭辞付与") =>
        targetData.split(':').lastOption.map(replacePrefix(d.defaultCode.get.value, _))
      case ("04", "項目選択") if targetData.startsWith("重複項目削除") =>
        Some(replaceDropDuplicate(d.defaultCode.get.value))
      case _ => convertScalaSource(baseUrl, branch)(cd, d)
    }
  }
}
*/