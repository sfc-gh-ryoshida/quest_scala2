package d2k.appdefdoc.finder

import Commons._

import scala.util.Try
import scala.reflect.io.Directory
import java.io.FileWriter
import org.apache.commons.io.output.FileWriterWithEncoding
import d2k.appdefdoc.parser._
import java.io.File
import d2k.appdefdoc.parser.D2kParser
import scala.io.Source

object ItemReferenceFinder extends App with D2kParser {
  val isLocalMode = args.size >= 4
  val (baseUrl, branch, targetItemId) = (args(0), args(1), args(2))
  val basePath = if (isLocalMode) args(3) else "C:/d2k_docs"
  val writeBase = s"data/irFinder/${targetItemId}"
  val writePath = Directory(writeBase)

  println(s"[Start Item Reference Finder${if (isLocalMode) " on Local Mode" else ""}] ${args.mkString(" ")}")

  val itemBasePath = s"${basePath}/apps/common"
  val itemNames = recList(new File(itemBasePath)).filter(_.getName.contains(".md"))
  val targetResouces = itemNames.filter { path =>
    Try { readItemDefMd(path.toString).contains(targetItemId) }.getOrElse({ println(s"  itemDef parse error: ${path}"); false })
  }.toList
  val itemDefList = targetResouces.flatMap(x => Try((x.toString, ItemDefParser(x.toString).get)).toOption).toList

  val appBasePath = s"${basePath}/apps"
  val appDefList = Commons.appDefList(appBasePath)

  val rrdList = itemDefList.flatMap { itemDef =>
    val targetName = itemDef._2.id
    val filtered = appDefList.filter(_._2.inputList.map(_.id).exists(_.contains(targetName.trim)))

    val result = createRrfData(targetName, filtered)
    val writeResult = writeRrfData(targetName, baseUrl, basePath, writePath, Some("    [Finish Resource Relation Finder]"), result)
    writeResult.map(x => (x._1, x._2))
  }

  val resultResource = rrdList.map {
    case (io, _) => mkTable(s"[${io.id}](${io.id}.md)", io.srcType, io.name)
  }

  val implList = implementList(targetItemId, appDefList)
  
  val tmpl = fileToStr("finderTemplates/irResult.tmpl")
  val writeFilePath = s"${writePath.toString}/README.md"
  writePath.createDirectory(true, false)
  val writer = new FileWriter(writeFilePath)
  val conved = tmpl.replaceAllLiterally("%%SearchTarget%%", targetItemId)
    .replaceAllLiterally("%%ResultResource%%", resultResource.mkString("\n"))
    .replaceAllLiterally("%%ResultItemReference%%", implList.map(_._2).mkString("\n"))
  writer.write(conved)
  writer.close

  val allAppNames = (rrdList.map(_._2.appInfo.id) ++ implList.map(_._1._2.appInfo.id)).toSet
  val allApps = allAppNames.map { x => appDefList.filter(_._2.appInfo.id == x).head }

  val csvImplTitle = Seq("Target Name", "App Id", "Sub App Id", "App Name", "Url", "Sub Url").mkString("", " , ", "\n")
  val csvImplData = implList.map {
    case (app, _, subImpl) =>
      val appdef = appDefList.filter(_._2.appInfo.id == app._2.appInfo.id).head
      val localPath = localPath2Url(baseUrl, basePath, appdef._1)
      Seq(targetItemId, app._2.appInfo.id, subImpl.dropRight(3), app._2.appInfo.name,
        localPath, s"${localPath.dropRight(9)}${subImpl}").mkString(" , ")
  }.mkString("\n")

  val csvImplFilePath = s"${writePath.toString}/impl.csv"
  val csvImplWriter = new FileWriterWithEncoding(csvImplFilePath, "MS932")
  csvImplWriter.write(csvImplTitle)
  csvImplWriter.write(csvImplData)
  csvImplWriter.write("\n")
  csvImplWriter.close

  val csvAllTitle = Seq("Target Name", "App Id", "App Name", "Url").mkString("", " , ", "\n")
  val csvAllData = allApps.map {
    case (path, appDef) =>
      Seq(targetItemId, appDef.appInfo.id, appDef.appInfo.name, localPath2Url(baseUrl, basePath, path)).mkString(" , ")
  }.mkString("\n")

  val csvAllFilePath = s"${writePath.toString}/all.csv"
  val csvAllWriter = new FileWriterWithEncoding(csvAllFilePath, "MS932")
  csvAllWriter.write(csvAllTitle)
  csvAllWriter.write(csvAllData)
  csvAllWriter.write("\n")
  csvAllWriter.close

  println(s"[Finish Item Reference Finder] ${pathOutputString(writeFilePath)}")
}
