package d2k.appdefdoc.finder.jsonbase

import d2k.appdefdoc.finder._
import d2k.appdefdoc.finder.{Commons => fcom}
import Commons._
import scala.util.Try
import scala.reflect.io.Directory
import java.io.FileWriter
import org.apache.commons.io.output.FileWriterWithEncoding
import d2k.appdefdoc.parser._
import java.io.File
import d2k.appdefdoc.parser.D2kParser
import scala.io.Source
import scala.annotation.tailrec
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object ResourceItemRouteFinder extends App with D2kParser {
   val isLocalMode = args.size >= 5

   val (baseUrl, branch, targetResourceId, targetItemId) = (args(0), args(1), args(2), args(3))

   val basePath = if (isLocalMode)
      args(4)
else
      "C:/d2k_docs"

   val writeBase = s"data/js/riRouteFinder/${ targetResourceId }_${ targetItemId }"

   val writePath = Directory(writeBase)

   val jsonPath = createJsonPath(basePath)

   println(s"[Start Resource Item Finder using json data${ if (isLocalMode)
      " on Local Mode"
else
      "" }] ${ args.mkString(" ") }")

   val itemBasePath = s"${ basePath }/apps/common"

   val itemNames = fcom.recList(new File (itemBasePath)).filter( x =>x.getName.contains(".md") && !x.getName.endsWith("README.md"))

   val itemDefMap = fcom.createItemDefMap(targetItemId, itemNames)

   val linkReadJson = createLinkReadJson(jsonPath)

   val linkWriteJson = createLinkWriteJson(jsonPath)

   val nodeAppMap = createNodeAppMap(jsonPath)

   val nodeResourceMap = createNodeResourceMap(jsonPath)

   val itemNamePath = createItemNamePathList(basePath)

   val jsonAppdef = createJsonAppdef(basePath, itemNamePath, nodeAppMap, nodeResourceMap, (linkReadJson ++ linkWriteJson).toList)

   def searchResource = jsonAppdef.find{
   case (_, appdef) => appdef.inputList.exists( x =>itemDefMap.keySet.contains(x.id))
   }

   val targetResource = itemDefMap(targetResourceId)

   val searchTargetResult = s"[${ targetResource.id }](${ targetResource.path })[${ targetResource.name }] / ${ targetItemId }[${ targetResource.itemName }]"

   val result = fcom.recursiveSearch(jsonAppdef.toList, itemDefMap, (appdef: AppDef) 
 =>appdef.inputList, (appdef: AppDef) =>appdef.outputList)(targetResourceId).distinct

   val flowRender = fcom.createFlowRender(result)

   val referResult = fcom.createReferResult(result)

   val appBasePath = s"${ basePath }/apps"

   val appDefList = fcom.appDefList(appBasePath)

   val appIds = result.flatMap(_.appDetail.map(_.appDef.appInfo.id))

   val filteredAppDefList = appDefList.filter( x =>appIds.contains(x._2.appInfo.id))

   val implList = fcom.implementList(targetItemId, filteredAppDefList)

   val implTableRender = implList.map(_._2).mkString("\n")

   val tmpl = fcom.fileToStr("finderTemplates/rirResult.tmpl")

   val writeFilePath = s"${ writePath.toString }/${ targetResourceId }_${ targetItemId }.md"

   writePath.createDirectory(true, false)

   val writer = new FileWriter (writeFilePath)

   val conved = tmpl.replaceAllLiterally("%%SearchTarget%%", searchTargetResult).replaceAllLiterally("%%ResultFlow%%", flowRender).replaceAllLiterally("%%ResultApplicationList%%", referResult).replaceAllLiterally("%%ResultItemReference%%", implTableRender)

   writer.write(conved)

   writer.close

   val csvReferTitle = Seq("Target Resource Id", "Target Item Id", "Input Resource Id", "Input Resource Name", "App Id", "App Name", "Resource Url", "App Url").mkString("", " , ", "\n")

   val csvReferData = result.map{ data => val resPath = fcom.localPath2Url(baseUrl, basePath, data.resDetail.path)
 val csvData = data.appDetail.map{ x => val appUrl = fcom.localPath2Url(baseUrl, basePath, x.path)
Seq(data.resDetail.id, data.resDetail.name, x.appDef.appInfo.id, x.appDef.appInfo.name, resPath, appUrl)
}.getOrElse(Seq(data.resDetail.id, data.resDetail.name, "", "", resPath, ""))
(Seq(targetResourceId, targetItemId) ++ csvData).mkString(" , ")
}.mkString("\n")

   val csvReferFilePath = s"${ writePath.toString }/refer.csv"

   val csvReferWriter = new FileWriterWithEncoding (csvReferFilePath, "MS932")

   csvReferWriter.write(csvReferTitle)

   csvReferWriter.write(csvReferData)

   csvReferWriter.write("\n")

   csvReferWriter.close

   val csvImplTitle = Seq("Target Resource Id", "Target Item Id", "App Id", "Sub App Id", "App Name", "Url", "Sub Url").mkString("", " , ", "\n")

   val csvImplData = implList.map{
   case ((path, appdef), _, subId) => val localPath = fcom.localPath2Url(baseUrl, basePath, path)
Seq(targetResourceId, targetItemId, appdef.appInfo.id, subId.dropRight(3), 
appdef.appInfo.name, localPath, s"${ localPath.dropRight(9) }${ subId }").mkString(" , ")
   }.mkString("\n")

   val csvImplFilePath = s"${ writePath.toString }/impl.csv"

   val csvImplWriter = new FileWriterWithEncoding (csvImplFilePath, "MS932")

   csvImplWriter.write(csvImplTitle)

   csvImplWriter.write(csvImplData)

   csvImplWriter.write("\n")

   csvImplWriter.close

   println(s"[Finish Resource Item Route Finder] ${ fcom.pathOutputString(writeFilePath) }")
}