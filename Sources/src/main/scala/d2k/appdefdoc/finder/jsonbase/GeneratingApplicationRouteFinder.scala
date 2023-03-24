package d2k.appdefdoc.finder.jsonbase

import d2k.appdefdoc.finder.{ Commons => fcom }
import d2k.appdefdoc.parser._
import d2k.appdefdoc.parser.D2kParser
import scala.reflect.io.Directory
import java.io.File
import scala.collection.Seq
import scala.reflect.io.Path.string2path
import scala.annotation.tailrec
import scala.util.Try
import java.io.FileWriter
import org.apache.commons.io.output.FileWriterWithEncoding

import Commons._
import d2k.appdefdoc.finder.RirfDetail

case class GafData(baseAppId: Option[String], baseResourceId: String, result: Seq[(String, AppDef)])
object GeneratingApplicationRouteFinder extends App with D2kParser {
  val isLocalMode = args.size >= 4
  val (baseUrl, branch, targetResourceId) = (args(0), args(1), args(2))
  val basePath = if (isLocalMode) args(3) else "C:/d2k_docs"
  val writeBase = s"data/js/gaRouteFinder/${targetResourceId}"
  val writePath = Directory(writeBase)
  val jsonPath = createJsonPath(basePath)

  println(s"[Start Generating Application Route Finder ${if (isLocalMode) " on Local Mode" else ""}] ${args.mkString(" ")}")

  val itemBasePath = s"${basePath}/apps/common"
  val itemNames = fcom.recList(new File(itemBasePath)).filter(x => x.getName.contains(".md") && !x.getName.endsWith("README.md"))

  val linkReadJson = createLinkReadJson(jsonPath)
  val linkWriteJson = createLinkWriteJson(jsonPath)

  val nodeAppMap = createNodeAppMap(jsonPath)
  val nodeResourceMap = createNodeResourceMap(jsonPath)

  val itemNamePath = createItemNamePathList(basePath)
  val targetResource = nodeResourceMap.get(targetResourceId)

  val searchTargetResult = nodeResourceMap.toList.find {
    case (k, v) => k.contains(targetResourceId)
  }.flatMap { node =>
    itemNamePath.find { p =>
      node._2.physical_name.contains(p._1)
    }.map { p =>
      s"[${node._2.physical_name}](${p._2})[${node._2.logical_name}]"
    }
  }.getOrElse(s"${targetResourceId}")

  val appBasePath = s"${basePath}/apps"

  val jsonAppdef = createJsonAppdef(basePath, itemNamePath, nodeAppMap, nodeResourceMap, (linkReadJson ++ linkWriteJson).toList)
  val itemDefMap = itemNamePath.flatMap {
    case (name, path) =>
      Try {
        val itemdef = ItemDefParser(path.toString).get
        itemdef.details.map { item =>
          (itemdef.id, RirfDetail(itemdef.id, itemdef.name, item.name, path.toString))
        }
      }.getOrElse { println(s"  itemDef parse error: ${path}"); Seq.empty[(String, RirfDetail)] }
  }.toMap

  val result = fcom.recursiveSearch(jsonAppdef.toList, itemDefMap,
    (appdef: AppDef) => appdef.outputList, (appdef: AppDef) => appdef.inputList)(targetResourceId).distinct

  val flowRender = fcom.createFlowRender(
    result,
    d => Seq(d.appDetail.map(x => s"${x.appDef.appInfo.id} --> ${d.resDetail.id}_res"), d.parentAppId.map(x => s"${d.resDetail.id}_res --> ${x}")).flatten)

  val referResult = fcom.createReferResult(result)

  val tmpl = fcom.fileToStr("finderTemplates/garResult.tmpl")
  val writeFilePath = s"${writePath.toString}/${targetResourceId}.md"
  writePath.createDirectory(true, false)
  val writer = new FileWriter(writeFilePath)
  val conved = tmpl.replaceAllLiterally("%%SearchTarget%%", searchTargetResult)
    .replaceAllLiterally("%%ResultFlow%%", flowRender)
    .replaceAllLiterally("%%ResultApplicationList%%", referResult)
  writer.write(conved)
  writer.close

  val csvReferTitle = Seq("Target Resource Id", "Output Resource Id", "Output Resource Name", "App Id", "App Name", "Resource Url", "App Url").mkString("", " , ", "\n")
  val csvReferData = result.map { data =>
    val resPath = fcom.localPath2Url(baseUrl, basePath, data.resDetail.path)
    val csvData = data.appDetail.map { x =>
      val appUrl = fcom.localPath2Url(baseUrl, basePath, x.path)
      Seq(targetResourceId, data.resDetail.id, data.resDetail.name, x.appDef.appInfo.id, x.appDef.appInfo.name, resPath, appUrl)
    }.getOrElse(Seq(targetResourceId, data.resDetail.id, data.resDetail.name, "", "", resPath, ""))
    csvData.mkString(" , ")
  }.mkString("\n")
  val csvReferFilePath = s"${writePath.toString}/refer.csv"
  val csvReferWriter = new FileWriterWithEncoding(csvReferFilePath, "MS932")
  csvReferWriter.write(csvReferTitle)
  csvReferWriter.write(csvReferData)
  csvReferWriter.write("\n")
  csvReferWriter.close

  println(s"[Finish Generating Application Route Finder] ${fcom.pathOutputString(writeFilePath)}")
}
