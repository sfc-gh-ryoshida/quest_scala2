package d2k.appdefdoc.finder

import d2k.appdefdoc.finder.Commons._
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
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class GafData (baseAppId: Option[String], baseResourceId: String, result: Seq[(String, AppDef)])
 object GeneratingApplicationRouteFinder extends App with D2kParser {
   val isLocalMode = args.size >= 4

   val (baseUrl, branch, targetResourceId) = (args(0), args(1), args(2))

   val basePath = if (isLocalMode)
      args(3)
else
      "C:/d2k_docs"

   val writeBase = s"data/gaRouteFinder/${ targetResourceId }"

   val writePath = Directory(writeBase)

   println(s"[Start Generating Application Route Finder ${ if (isLocalMode)
      " on Local Mode"
else
      "" }] ${ args.mkString(" ") }")

   val appBasePath = s"${ basePath }/apps"

   val appDefList = Commons.appDefList(appBasePath)

   def searchResource(resourceName: String) = appDefList.filter{
   case (_, appdef) => val existCheck = appdef.outputList.filter(_.id == resourceName)
!existCheck.isEmpty
   }

   val itemBasePath = s"${ basePath }/apps/common"

   val itemNames = recList(new File (itemBasePath)).filter( x =>x.getName.contains(".md") && !x.getName.endsWith("README.md"))

   val itemDefMap = itemNames.flatMap{ path =>Try{
   val itemdef = ItemDefParser(path.toString).get
itemdef.details.map{ item =>(itemdef.id, RirfDetail(itemdef.id, itemdef.name, item.name, path.toString))
}
   }.getOrElse{
   println(s"  itemDef parse error: ${ path }");Seq.empty[(String, RirfDetail)]
   }
}.toMap

   val targetResource = itemDefMap(targetResourceId)

   val searchTargetResult = s"[${ targetResource.id }](${ targetResource.path })[${ targetResource.name }]"

   val result = recursiveSearch(appDefList, itemDefMap, (appdef: AppDef) 
 =>appdef.outputList, (appdef: AppDef) =>appdef.inputList)(targetResourceId).distinct

   val flowRender = createFlowRender(
result, d 
 =>Seq(d.appDetail.map( x =>s"${ x.appDef.appInfo.id } --> ${ d.resDetail.id }"), d.parentAppId.map( x =>s"${ d.resDetail.id } --> ${ x }")).flatten)

   val referResult = createReferResult(result)

   val tmpl = fileToStr("finderTemplates/garResult.tmpl")

   val writeFilePath = s"${ writePath.toString }/${ targetResourceId }.md"

   writePath.createDirectory(true, false)

   val writer = new FileWriter (writeFilePath)

   val conved = tmpl.replaceAllLiterally("%%SearchTarget%%", searchTargetResult).replaceAllLiterally("%%ResultFlow%%", flowRender).replaceAllLiterally("%%ResultApplicationList%%", referResult)

   writer.write(conved)

   writer.close

   val csvReferTitle = Seq("Target Resource Id", "Output Resource Id", "Output Resource Name", "App Id", "App Name", "Resource Url", "App Url").mkString("", " , ", "\n")

   val csvReferData = result.map{ data => val resPath = localPath2Url(baseUrl, basePath, data.resDetail.path)
 val csvData = data.appDetail.map{ x => val appUrl = localPath2Url(baseUrl, basePath, x.path)
Seq(targetResourceId, data.resDetail.id, data.resDetail.name, x.appDef.appInfo.id, x.appDef.appInfo.name, resPath, appUrl)
}.getOrElse(Seq(targetResourceId, data.resDetail.id, data.resDetail.name, "", "", resPath, ""))
csvData.mkString(" , ")
}.mkString("\n")

   val csvReferFilePath = s"${ writePath.toString }/refer.csv"

   val csvReferWriter = new FileWriterWithEncoding (csvReferFilePath, "MS932")

   csvReferWriter.write(csvReferTitle)

   csvReferWriter.write(csvReferData)

   csvReferWriter.write("\n")

   csvReferWriter.close

   println(s"[Finish Generating Application Route Finder] ${ pathOutputString(writeFilePath) }")
}