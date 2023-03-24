package d2k.appdefdoc.gen.tmpl

import scala.io.Source
import scala.reflect.io.Directory
import java.io.FileWriter
import scala.reflect.io.Path.string2path

case class CatalogInfo(
  name: String, data: String)

object CatalogMaker extends App {
  println("create templates start")
  def fileToStr(fileName: String) =
    Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName)).mkString

  val base = fileToStr("templates/base.md")

  val readDb = fileToStr("templates/readDb.md")
  val singleReadDb = readDb.replaceAll("%%name%%", fileToStr("templates/singleReadDb.md"))
  val multiReadDb = readDb.replaceAll("%%name%%", fileToStr("templates/multiReadDb.md"))
  val readFile = fileToStr("templates/readFile.md")
  val readPq = fileToStr("templates/readPq.md")
  val singleReadPq = readPq.replaceAll("%%name%%", fileToStr("templates/singleReadPq.md"))
  val multiReadPq = readPq.replaceAll("%%name%%", fileToStr("templates/multiReadPq.md"))

  val joinDf = fileToStr("templates/joinDf.md")
  val joinPq = fileToStr("templates/joinPq.md")
  val unionDf = fileToStr("templates/unionDf.md")

  val writeDb = fileToStr("templates/writeDb.md")
  val writeFile = fileToStr("templates/writeFile.md")
  val writePq = fileToStr("templates/writePq.md")
  val toVal = fileToStr("templates/toVal.md")

  val ins = List(
    CatalogInfo("Db", singleReadDb),
    CatalogInfo("File", readFile),
    CatalogInfo("Pq", singleReadPq),
    CatalogInfo("Df", ""))

  val outs = List(
    CatalogInfo("Db", writeDb),
    CatalogInfo("File", writeFile),
    CatalogInfo("Pq", writePq),
    CatalogInfo("Val", toVal),
    CatalogInfo("Df", ""))

  val writePath = Directory("catalogs/02_templates")
  writePath.deleteRecursively
  writePath.createDirectory(true, false)

  def makeTemplate(i: CatalogInfo, o: CatalogInfo) = {
    val templateName = s"${i.name}To${o.name}"
    val fileName = s"_${templateName}.md"
    val repStr = i.data + o.data

    val writer = new FileWriter((writePath / fileName).toString)
    writer.write(
      base.replaceAll("%%templatePattern%%", templateName).replaceAll("%%insert%%", repStr))
    writer.close
  }

  for {
    i <- ins
    o <- outs
  } makeTemplate(i, o)

  makeTemplate(CatalogInfo("MultiDb", multiReadDb), CatalogInfo("MapDf", ""))
  makeTemplate(CatalogInfo("MultiPq", multiReadPq), CatalogInfo("MapDf", ""))

  makeTemplate(CatalogInfo("DfJoin", joinDf), CatalogInfo("Df", ""))
  makeTemplate(CatalogInfo("PqJoin", joinPq), CatalogInfo("Pq", writePq))
  makeTemplate(CatalogInfo("DfUnion", unionDf), CatalogInfo("Df", ""))

  println("create templates finish")
}
