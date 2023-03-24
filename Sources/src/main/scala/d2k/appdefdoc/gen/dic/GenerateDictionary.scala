package d2k.appdefdoc.gen.dic

import d2k.common.MakeResource
import scala.io.Source
import scala.reflect.io.Path
import scala.reflect.io.Path.string2path
import d2k.appdefdoc.parser.ItemDefParser
import java.io.FileWriter
import spark.common.FileCtl
import scala.reflect.io.Directory

object GenerateDictionary {
  def apply(baseUrl: String) = new GenerateDictionary(baseUrl)
}

case class DicData(id: String, name: String, dic: String) {
  override def toString = Seq(id, dic, "固有名詞", name).mkString("\t")
}
class GenerateDictionary(baseUrl: String) {
  def generate(branch: String) = {
    val dbResult = generateDictionary(baseUrl, branch, "db")
    val pqResult = generateDictionary(baseUrl, branch, "pq")
    val result = (dbResult ++ pqResult).groupBy(d => (d.id, d.dic)).map { case (k, d) => d.head }.toList

    val outputPath = "data/dicGen"
    val outputFile = s"${outputPath}/d2k_appdef.txt"
    Directory(outputPath).createDirectory(true, false)
    FileCtl.writeToFile(outputFile, false, "MS932") { w =>
      result.sortBy(_.id).foreach(w.println)
    }
    println(s"[Finish Dictionary Generate] ${outputFile}")
  }

  def generateDictionary(baseUrl: String, branch: String, category: String) = {
    val itemsUrl = s"${baseUrl}/raw/${branch}/apps/common/items/${category}"
    val url = s"${itemsUrl}/README.md"

    val dicConv = dicConvertPattern(baseUrl, "master")

    val md = Source.fromURL(s"${url}?private_token=${sys.env("GITLAB_TOKEN")}").getLines.toList
    val grpList = md.filter(!_.isEmpty).dropWhile(line => !line.contains("## 業務グループ一覧"))
      .drop(3).map(_.split('|')(1).trim.split('(')(1).dropRight(1))
    val files = fileList(itemsUrl, grpList.head)
    (for {
      g <- grpList
      f <- fileList(itemsUrl, g)
    } yield {
      makeDic(dicConv, baseUrl, branch, category, g.split('/')(0), f)
    }).flatten
  }

  def fileList(baseUrl: String, grpReadme: String) = {
    val grpUrl = s"${baseUrl}/${grpReadme}"
    val md = Source.fromURL(s"${grpUrl}?private_token=${sys.env("GITLAB_TOKEN")}").getLines.toList
    md.filter(!_.isEmpty).dropWhile(line => !line.contains("## "))
      .drop(3).map(_.split('|')(1).trim.split('(')(1).dropRight(1))
  }

  def makeDic(dicConv: Map[String, String], baseUrl: String, branch: String, category: String, grpName: String, fileName: String) = {
    val dicKeys = dicConv.keys.toList
    val itemdef = ItemDefParser(baseUrl, branch, s"${category}/${grpName}/${fileName}").get
    itemdef.details.map { items =>
      val target = dicConv.keys.filter(k => itemdef.id.toLowerCase.startsWith(k)).head
      val conved = itemdef.id.toLowerCase.replaceAllLiterally(target, dicConv(target))
      DicData(conved, itemdef.name, s"${items.id}[${items.name}]")
    }
  }

  def dicConvertPattern(baseUrl: String, branch: String) = {
    val dicConvUrl = s"${baseUrl}/raw/${branch}/guide/dicgen/dicPattern.md"
    val md = Source.fromURL(s"${dicConvUrl}?private_token=${sys.env("GITLAB_TOKEN")}").getLines.toList
    md.filter(!_.isEmpty).dropWhile(line => !line.contains("## 項目変換パターン"))
      .drop(3).map { x =>
        val splitted = x.split('|')
        (splitted(1).trim -> splitted(2).trim)
      }.toMap
  }
}
