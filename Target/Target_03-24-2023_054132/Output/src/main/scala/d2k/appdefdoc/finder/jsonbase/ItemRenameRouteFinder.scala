package d2k.appdefdoc.finder.jsonbase

import d2k.appdefdoc.finder._
import d2k.appdefdoc.finder.{Commons => fcom}
import Commons._
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
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object ItemRenameRouteFinder extends App with D2kParser {
   val isLocalMode = args.size >= 5

   val (baseUrl, branch, targetResourceId, targetItemId) = (args(0), args(1), args(2), args(3))

   val basePath = if (isLocalMode)
      args(4)
else
      "C:/d2k_docs"

   val jsonPath = createJsonPath(basePath)

   val writeBase = s"data/js/irRouteFinder/${ targetResourceId }_${ targetItemId }"

   val writePath = Directory(writeBase)

   println(s"[Start Item Rename RouteFinder${ if (isLocalMode)
      " on Local Mode"
else
      "" }] ${ args.mkString(" ") }")

   val itemBasePath = s"${ basePath }/apps/common"

   val itemFileNames = fcom.recList(new File (itemBasePath)).filter( x =>x.getName.contains(".md") && !x.getName.endsWith("README.md"))

   val itemDefMap = fcom.createItemDefMap(targetItemId, itemFileNames)

   val appBasePath = s"${ basePath }/apps"

   val appDefList = fcom.appDefList(appBasePath)

   val linkReadJson = createLinkReadJson(jsonPath)

   val linkWriteJson = createLinkWriteJson(jsonPath)

   val nodeAppMap = createNodeAppMap(jsonPath)

   val nodeResourceMap = createNodeResourceMap(jsonPath)

   val itemNamePath = createItemNamePathList(basePath)

   val jsonAppdefNoFlow = createJsonAppdef(basePath, itemNamePath, nodeAppMap, nodeResourceMap, (linkReadJson ++ linkWriteJson).toList)

   val jsonAppdef = mergeAppdef(jsonAppdefNoFlow.toList, appDefList)

   itemDefMap.get(targetResourceId).map{ targetResource => val searchTargetResult = s"[${ targetResource.id }](${ targetResource.path })[${ targetResource.name }] / ${ targetItemId }[${ targetResource.itemName }]"
 val result = fcom.recursiveSearchWithRename(jsonAppdef.toList, itemDefMap, (appdef: AppDef) 
 =>appdef.inputList, (appdef: AppDef) =>appdef.outputList, targetItemId, itemFileNames)(targetResourceId).distinct
 val flowRender = fcom.createFlowRenderWithRename(result)
 val referResult = fcom.createReferResult(result.map(_.appData))
 val renameAppList = result.flatMap{ result => val rirfData = result.appData
result.renameApps.flatMap{ renameData =>rirfData.appDetail.map{ rirfData =>(rirfData.appDef.appInfo.id, rirfData.path, rirfData.appDef.appInfo.name, 
renameData.componentId, s"${ rirfData.path.dropRight(9) }${ renameData.componentId }.md", renameData.beforeName, renameData.afterName)
}
}
}
 val renameAppTableList = renameAppList.map{ x =>fcom.mkTable(s"[${ x._1 }](${ x._2 })", x._3, s"[${ x._4 }](${ x._5 })", x._6, x._7)
}.mkString("\n")
 val tmpl = fcom.fileToStr("finderTemplates/irrResult.tmpl")
 val writeFilePath = s"${ writePath.toString }/${ targetResourceId }_${ targetItemId }.md"
writePath.createDirectory(true, false)
 val writer = new FileWriter (writeFilePath)
 val conved = tmpl.replaceAllLiterally("%%SearchTarget%%", searchTargetResult).replaceAllLiterally("%%ResultFlow%%", flowRender).replaceAllLiterally("%%ResultApplicationList%%", referResult).replaceAllLiterally("%%ResultItemRenameApplicationList%%", renameAppTableList)
writer.write(conved)
writer.close
 val csvReferTitle = Seq("Target Resource Id", "Target Item Id", "Input Resource Id", "Input Resource Name", "App Id", "App Name", "Resource Url", "App Url").mkString("", " , ", "\n")
 val csvReferData = result.map{ data => val resPath = fcom.localPath2Url(baseUrl, basePath, data.appData.resDetail.path)
 val csvData = data.appData.appDetail.map{ x => val appUrl = fcom.localPath2Url(baseUrl, basePath, x.path)
Seq(data.appData.resDetail.id, data.appData.resDetail.name, x.appDef.appInfo.id, x.appDef.appInfo.name, resPath, appUrl)
}.getOrElse(Seq(data.appData.resDetail.id, data.appData.resDetail.name, "", "", resPath, ""))
(Seq(targetResourceId, targetItemId) ++ csvData).mkString(" , ")
}.mkString("\n")
 val csvReferFilePath = s"${ writePath.toString }/refer.csv"
 val csvReferWriter = new FileWriterWithEncoding (csvReferFilePath, "MS932")
csvReferWriter.write(csvReferTitle)
csvReferWriter.write(csvReferData)
csvReferWriter.write("\n")
csvReferWriter.close
 val csvImplTitle = Seq("App Id", "App Name", "Sub App Id", "Url", "Sub Url", "Before Name", "After Name").mkString("", " , ", "\n")
 val csvImplData = renameAppList.map{
   case (appid, path, appName, subAppid, subAppPath, beforeName, afterName) => val localAppPath = fcom.localPath2Url(baseUrl, basePath, path)
 val localSubAppPath = fcom.localPath2Url(baseUrl, basePath, subAppPath)
Seq(appid, appName, subAppid, localAppPath, localSubAppPath, beforeName, afterName).mkString(" , ")
   }.mkString("\n")
 val csvImplFilePath = s"${ writePath.toString }/rename.csv"
 val csvImplWriter = new FileWriterWithEncoding (csvImplFilePath, "MS932")
csvImplWriter.write(csvImplTitle)
csvImplWriter.write(csvImplData)
csvImplWriter.write("\n")
csvImplWriter.close
println(s"[Finish Item Rename RouteFinder] ${ fcom.pathOutputString(writeFilePath) }")
}.orElse{
   println(s"[Error Item Rename RouteFinder] Not Found ${ targetResourceId } or ${ targetItemId }")
None
   }
}