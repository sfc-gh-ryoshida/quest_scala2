package d2k.appdefdoc.gen.sql

import scala.io.Source
import scala.reflect.io.Directory
import java.io.FileWriter
import scala.collection.Seq
import scala.ref

import d2k.appdefdoc.parser.D2kParser
import d2k.appdefdoc.parser.IoData

object SqlGenerator extends App {
  val isLocalMode = args.size >= 5
  val (baseUrl, branch, appGroup, appId) = (args(0), args(1), args(2), args(3))
  val basePath = if (isLocalMode) args(4) else ""
  val writeBase = s"data/sqlGen/${appGroup}"
  val writePath = Directory(writeBase)
  writePath.createDirectory(true, false)

  println(s"[Start Sql Generate${if (isLocalMode) " on Local Mode" else ""}] ${args.mkString(" ")}")
  val (appdef, sqlLogic) = if (isLocalMode) {
    (SqlDefParser(basePath, appGroup, appId).get, SqlLogicParser(basePath, appGroup, appId))
  } else {
    (SqlDefParser(baseUrl, branch, appGroup, appId).get, SqlLogicParser(baseUrl, branch, appGroup, appId))
  }
  val appdefStr = generate(appdef, sqlLogic)

  val writeFilePath = s"${writeBase}/${appId}.sql"
  val writer = new FileWriter(writeFilePath)
  writer.write(appdefStr)
  writer.close

  println(s"[Finish Sql Generate] ${writeFilePath}")

  def fileToStr(fileName: String) =
    Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName)).mkString

  def generate(appdef: SqlDef, sqlLogic: String) = {
    val mainTmpl = fileToStr("genTemplates/sqlMain.tmpl")
    val mainRepList = Seq(
      ("%%appId%%", appdef.appInfo.id),
      ("%%appDesc%%", appdef.appInfo.desc),
      ("%%sql%%", sqlLogic))
    mainRepList.foldLeft(mainTmpl) { (l, r) =>
      l.replaceAllLiterally(r._1, r._2)
    }
  }
}
