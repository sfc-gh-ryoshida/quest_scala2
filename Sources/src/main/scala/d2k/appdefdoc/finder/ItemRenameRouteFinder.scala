package d2k.appdefdoc.finder

import d2k.appdefdoc.finder.Commons._
import d2k.appdefdoc.parser._
import d2k.appdefdoc.parser.D2kParser
import scala.util.Try
import scala.reflect.io.Directory
import java.io.File
import scala.io.Source
import scala.collection.TraversableOnce.flattenTraversableOnce
import scala.reflect.io.Path.string2path
import org.apache.commons.io.output.FileWriterWithEncoding
import java.io.FileWriter
import scala.annotation.tailrec

case class IrrfData(appData: RirfData, renameApps: Seq[RenameData])
case class RenameData(componentId: String, beforeName: String, afterName: String)
object ItemRenameRouteFinder extends App with D2kParser {
  val isLocalMode = args.size >= 5
  val (baseUrl, branch, targetResourceId, targetItemId) = (args(0), args(1), args(2), args(3))
  val basePath = if (isLocalMode) args(4) else "C:/d2k_docs"
  val writeBase = s"data/irRouteFinder/${targetResourceId}_${targetItemId}"
  val writePath = Directory(writeBase)

  println(s"[Start Item Rename RouteFinder${if (isLocalMode) " on Local Mode" else ""}] ${args.mkString(" ")}")

  val itemBasePath = s"${basePath}/apps/common"
  val itemFileNames = recList(new File(itemBasePath)).filter(x => x.getName.contains(".md") && !x.getName.endsWith("README.md"))
  val itemDefMap = createItemDefMap(targetItemId, itemFileNames)

  val appBasePath = s"${basePath}/apps"
  val appDefList = Commons.appDefList(appBasePath)

  itemDefMap.get(targetResourceId).map { targetResource =>
    val searchTargetResult = s"[${targetResource.id}](${targetResource.path})[${targetResource.name}] / ${targetItemId}[${targetResource.itemName}]"
    val result = recursiveSearchWithRename(appDefList, itemDefMap,
      (appdef: AppDef) => appdef.inputList, (appdef: AppDef) => appdef.outputList, targetItemId, itemFileNames)(targetResourceId).distinct

    val flowRender = createFlowRenderWithRename(result)
    val referResult = createReferResult(result.map(_.appData))
    val renameAppList = result.flatMap { result =>
      val rirfData = result.appData
      result.renameApps.flatMap { renameData =>
        rirfData.appDetail.map { rirfData =>
          (rirfData.appDef.appInfo.id, rirfData.path, rirfData.appDef.appInfo.name,
            renameData.componentId, s"${rirfData.path.dropRight(9)}${renameData.componentId}.md", renameData.beforeName, renameData.afterName)
        }
      }
    }

    val renameAppTableList = renameAppList.map { x =>
      mkTable(s"[${x._1}](${x._2})", x._3, s"[${x._4}](${x._5})", x._6, x._7)
    }.mkString("\n")

    val tmpl = fileToStr("finderTemplates/irrResult.tmpl")
    val writeFilePath = s"${writePath.toString}/${targetResourceId}_${targetItemId}.md"
    writePath.createDirectory(true, false)
    val writer = new FileWriter(writeFilePath)
    val conved = tmpl.replaceAllLiterally("%%SearchTarget%%", searchTargetResult)
      .replaceAllLiterally("%%ResultFlow%%", flowRender)
      .replaceAllLiterally("%%ResultApplicationList%%", referResult)
      .replaceAllLiterally("%%ResultItemRenameApplicationList%%", renameAppTableList)
    writer.write(conved)
    writer.close

    val csvReferTitle = Seq("Target Resource Id", "Target Item Id", "Input Resource Id", "Input Resource Name", "App Id", "App Name", "Resource Url", "App Url").mkString("", " , ", "\n")
    val csvReferData = result.map { data =>
      val resPath = localPath2Url(baseUrl, basePath, data.appData.resDetail.path)
      val csvData = data.appData.appDetail.map { x =>
        val appUrl = localPath2Url(baseUrl, basePath, x.path)
        Seq(data.appData.resDetail.id, data.appData.resDetail.name, x.appDef.appInfo.id, x.appDef.appInfo.name, resPath, appUrl)
      }.getOrElse(Seq(data.appData.resDetail.id, data.appData.resDetail.name, "", "", resPath, ""))
      (Seq(targetResourceId, targetItemId) ++ csvData).mkString(" , ")
    }.mkString("\n")
    val csvReferFilePath = s"${writePath.toString}/refer.csv"
    val csvReferWriter = new FileWriterWithEncoding(csvReferFilePath, "MS932")
    csvReferWriter.write(csvReferTitle)
    csvReferWriter.write(csvReferData)
    csvReferWriter.write("\n")
    csvReferWriter.close

    val csvImplTitle = Seq("App Id", "App Name", "Sub App Id", "Url", "Sub Url", "Before Name", "After Name").mkString("", " , ", "\n")
    val csvImplData = renameAppList.map {
      case (appid, path, appName, subAppid, subAppPath, beforeName, afterName) =>
        val localAppPath = localPath2Url(baseUrl, basePath, path)
        val localSubAppPath = localPath2Url(baseUrl, basePath, subAppPath)
        Seq(appid, appName, subAppid, localAppPath, localSubAppPath, beforeName, afterName).mkString(" , ")
    }.mkString("\n")
    val csvImplFilePath = s"${writePath.toString}/rename.csv"
    val csvImplWriter = new FileWriterWithEncoding(csvImplFilePath, "MS932")
    csvImplWriter.write(csvImplTitle)
    csvImplWriter.write(csvImplData)
    csvImplWriter.write("\n")
    csvImplWriter.close

    println(s"[Finish Item Rename RouteFinder] ${pathOutputString(writeFilePath)}")

  }.orElse {
    println(s"[Error Item Rename RouteFinder] Not Found ${targetResourceId} or ${targetItemId}")
    None
  }
}


