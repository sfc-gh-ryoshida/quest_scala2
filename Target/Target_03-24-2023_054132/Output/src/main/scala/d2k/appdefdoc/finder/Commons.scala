package d2k.appdefdoc.finder

import scala.io.Source
import scala.reflect.io.Directory
import java.io.FileWriter
import d2k.appdefdoc.parser.AppDef
import org.apache.commons.io.output.FileWriterWithEncoding
import java.io.File
import scala.util.Try
import d2k.appdefdoc.parser._
import scala.annotation.tailrec
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class RirfFlow (kind: String, id: String, name: String = "")
case class IrrfFlow (rirfFlow: RirfFlow, beforeName: String, afterName: String)
 object Commons {
   def fileToStr(fileName: String) = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName)).mkString

   def mkTable(data: String*) = data.mkString("| ", " | ", " |")

   def pathOutputString(path: String) = path.replaceAllLiterally("\\", "/")

   def localPath2Url(baseUrl: String, basePath: String, localPath: String) = s"${ baseUrl }/${ localPath.replaceAllLiterally("\\", "/").replaceAllLiterally(basePath.replaceAllLiterally("\\", "/"), "tree/master") }"

   def recList(file: File) : Array[File] = {
   val files = file.listFiles
files ++ files.filter(_.isDirectory).flatMap(recList)
   }

   val appDefRegx = """.*\\apps\\\w{3}\d*\\\w+\\README.md""".r

   def appDefList(appBasePath: String) = {
   recList(new File (appBasePath)).filter(_.getName.contains("README.md")).filter( x =>appDefRegx.findFirstMatchIn(x.toString).isDefined).flatMap( x =>Try((x.toString, AppDefParser(x.toString).get)).toOption.orElse{
      println(s"  appDef parse error: ${ x.toString }");None
      }).toList
   }

   def createRrfData(targetName: String, appDefList: Seq[(String, AppDef)]) = {
   appDefList.map{ x => val (path, appdef) = x
 val in = appdef.inputList.map(_.id).contains(targetName)
 val out = appdef.outputList.map(_.id).contains(targetName)
 val containType = (in, out) match {
         case (false, false) => "none"
         case (true, false) => "in"
         case (false, true) => "out"
         case (true, true) => "io"
      }
 val ioData = appdef.inputList.filter(_.id == targetName).headOption.orElse(appdef.outputList.filter(_.id == targetName).headOption)
RrfData(path, appdef.appInfo, ioData, containType)
}.filter(_.containType != "none")
   }

   def writeRrfData(targetName: String, baseUrl: String, basePath: String, writePath: Directory, finishMessage: Option[String], rrfData: Seq[RrfData]) = {
   rrfData.headOption.map{ r =>writePath.createDirectory(true, false)
 val targetObj = r.ioData.get
 val targetObjTitle = if (targetObj.path.isEmpty)
         {
         val appInfo = s"${ rrfData.head.ioData.get.id }[${ targetObj.name }]"
println(s"  appDef not found: ${ appInfo }")
appInfo
         }
else
         {
         val targetObjPath = s"${ basePath }/apps/common/${ targetObj.path.split("/common/")(1) }"s"[${ rrfData.head.ioData.get.id }](${ targetObjPath })[${ targetObj.name }]"
         }
 val targetObjUml = s"""artifact "${ targetObj.id }\\n${ targetObj.name }" as ${ targetObj.id }_res"""
 val appUml = rrfData.map{ d =>s"[${ d.appInfo.id }\\n${ d.appInfo.name }] as ${ d.appInfo.id }"
}
 val chainUml = rrfData.map{ d =>d.containType match {
         case "in" => s"${ targetObj.id }_res --> ${ d.appInfo.id } :Input"
         case "out" => s"${ d.appInfo.id } --> ${ targetObj.id }_res :Output"
         case "io" => s"${ targetObj.id }_res --> ${ d.appInfo.id } :Input\\n${ d.appInfo.id } --> ${ targetObj.id }_res :Output"
         case _ => ""
      }
}
 val umls = targetObjUml :: (appUml ++ chainUml).toList
 def dataToTable(rrf: RrfData) = if (rrf.path.isEmpty)
         {
         println(s"  appDef not found: ${ rrf.appInfo.id }[${ rrf.appInfo.name }]")
s"| ${ rrf.appInfo.id } | ${ rrf.appInfo.name } |"
         }
else
         {
         s"| [${ rrf.appInfo.id }](${ rrf.path }) | ${ rrf.appInfo.name } |"
         }
 val outputTables = rrfData.filter(_.containType == "out").map(dataToTable)
 val inputTables = rrfData.filter(_.containType == "in").map(dataToTable)
 val tmpl = fileToStr("finderTemplates/rrResult.tmpl")
 val writeFilePath = s"${ writePath.toString }/${ targetObj.id }.md"
 val writer = new FileWriter (writeFilePath)
 val conved = tmpl.replaceAllLiterally("%%SearchTarget%%", targetObjTitle).replaceAllLiterally("%%ResultPlantuml%%", umls.mkString("\n")).replaceAllLiterally("%%ResultOutput%%", outputTables.mkString("\n")).replaceAllLiterally("%%ResultInput%%", inputTables.mkString("\n"))
writer.write(conved)
writer.close
 val csvTitle = Seq("Target Name", "App Id", "App Name", "Io Type", "Url").mkString("", " , ", "\n")
 val csvData = rrfData.map{ rrf => val path = if (rrf.path.isEmpty)
         {
         ""
         }
else
         {
         localPath2Url(baseUrl, basePath, rrf.path)
         }
Seq(targetName, rrf.appInfo.id, rrf.appInfo.name, rrf.containType, path).mkString(" , ")
}.mkString("\n")
 val writeCsvFilePath = s"${ writePath.toString }/${ targetObj.id }.csv"
 val csvWriter = new FileWriterWithEncoding (writeCsvFilePath, "MS932")
csvWriter.write(csvTitle)
csvWriter.write(csvData)
csvWriter.write("\n")
csvWriter.close
finishMessage.foreach( mes =>println(s"${ mes } ${ pathOutputString(writeFilePath) }"))
Some(targetObj, r)
}.getOrElse({
      finishMessage.foreach( mes =>println(s"${ mes } Not Found Application. target[${ targetName }]"));None
      })
   }

   def implementList(targetItemId: String, appdefList: List[(String, AppDef)]) = appdefList.flatMap{
   case (appdefpath, appdef) => val compolist = appdef.componentList
 val result = compolist.flatMap{ x => val path = appdefpath.dropRight(9) + x.mdName
 val str = Source.fromFile(path).getLines.mkString
if (str.contains(targetItemId))
      Some(x.mdName)
else
      None
}
result.map{ subId => val subPath = s"${ appdefpath.dropRight(9) }/${ subId }"
 val outputTable = mkTable(s"[${ appdef.appInfo.id }](${ appdefpath }) / [${ subId.dropRight(3) }](${ subPath })", appdef.appInfo.name)
((appdefpath, appdef), outputTable, subId)
}
   }

   def createItemDefMap(targetItemId: String, itemNames: Array[File]) = {
   itemNames.flatMap{ path =>Try{
      val itemdef = ItemDefParser(path.toString).get
 val itemDetail = itemdef.details.find(_.id == targetItemId)
itemDetail.map{ item =>(itemdef.id, RirfDetail(itemdef.id, itemdef.name, item.name, path.toString))
}
      }.getOrElse{
      println(s"  itemDef parse error: ${ path }");None
      }
}.toMap
   }

   val maxDepth = 10

   def recursiveSearch(appDefList: List[(String, AppDef)], itemDefMap: Map[String, RirfDetail], appFind: AppDef => Seq[IoData], resourceFind: AppDef => Seq[IoData], depth: Int = 0)(targetResourceId: String, targetAppId: Option[String] = None) : Seq[RirfData] = {
   val itemDefMapKeys = itemDefMap.keySet
 val targetApp = appDefList.filter{
      case (path, appdef) => appFind(appdef).exists(_.id == targetResourceId)
      }
 val result = targetApp.flatMap{ appdef => val resources = resourceFind(appdef._2).filter( x =>itemDefMapKeys.exists(_ == x.id))
 val resResult = resources.foldLeft(Seq.empty[RirfData]){(l, r) =>if (depth > maxDepth)
         {
         l
         }
else
         {
         l ++ recursiveSearch(appDefList, itemDefMap, appFind, resourceFind, depth + 1)(r.id, Some(appdef._2.appInfo.id))
         }
}
RirfData(itemDefMap.getOrElse(targetResourceId, RirfDetail(targetResourceId)), Some(RirfAppDetail(appdef._2, appdef._1)), targetAppId) +: resResult
}
if (result.isEmpty)
         Seq(RirfData(itemDefMap.getOrElse(targetResourceId, RirfDetail(targetResourceId)), None, targetAppId))
else
         result
   }

   @tailrec private [this] def renameSearch(data: Map[String, String], targetId: String) : String = {
   val v = data.get(targetId)
if (v.isEmpty)
         targetId
else
         renameSearch((data - targetId), v.get)
   }

   val renameRegx = """####\s+\d{3}\s+:\s+(\w+)\[.*\]\s+->\s+(\w+)\[.*\].*""".r

   def componentDetailData(app: (String, AppDef), targetItemId: String) = {
   app._2.componentList.scanLeft(RenameData("", targetItemId, targetItemId)){(l, r) => val path = app._1.dropRight(9) + r.mdName
 val str = Source.fromFile(path).getLines.mkString("\n")
 val regxResult = renameRegx.findAllMatchIn(str).map{ x =>(x.group(1), x.group(2))}.toMap
RenameData(r.id, l.afterName, renameSearch(regxResult, l.afterName))
}
   }

   def recursiveSearchWithRename(appDefList: List[(String, AppDef)], itemDefMap: Map[String, RirfDetail], appFind: AppDef => Seq[IoData], resourceFind: AppDef => Seq[IoData], targetItemId: String, itemFileNames: Array[File], depth: Int = 0)(targetResourceId: String, targetAppId: Option[String] = None) : Seq[IrrfData] = {
   val targetApp = appDefList.filter{
      case (path, appdef) => appFind(appdef).exists(_.id == targetResourceId)
      }
 val result = targetApp.flatMap{ appdef => val renamedItemIdList = componentDetailData(appdef, targetItemId)
 val renameComponentList = renamedItemIdList.filter( x =>!x.componentId.isEmpty && x.beforeName != x.afterName)
 val filteredItemDefMap = createItemDefMap(renamedItemIdList.last.afterName, itemFileNames)
 val itemDefMapKeys = filteredItemDefMap.keySet
 val resources = resourceFind(appdef._2).filter( x =>itemDefMapKeys.exists(_ == x.id))
 val resResult = resources.foldLeft(Seq.empty[IrrfData]){(l, r) =>if (depth > 3)
         {
         l
         }
else
         {
         l ++ recursiveSearchWithRename(appDefList, itemDefMap, appFind, resourceFind, targetItemId, itemFileNames, depth + 1)(r.id, Some(appdef._2.appInfo.id))
         }
}
IrrfData(RirfData(filteredItemDefMap.getOrElse(targetResourceId, RirfDetail(targetResourceId)), Some(RirfAppDetail(appdef._2, appdef._1)), targetAppId), renameComponentList) +: resResult
}
if (result.isEmpty)
         Seq(IrrfData(RirfData(itemDefMap.getOrElse(targetResourceId, RirfDetail(targetResourceId)), None, targetAppId), Seq.empty[RenameData]))
else
         result
   }

   def createFlowRender(result: Seq[RirfData], flowLinkFunc: RirfData => Seq[String] = d =>Seq(d.appDetail.map( x =>s"${ d.resDetail.id }_res --> ${ x.appDef.appInfo.id }"), d.parentAppId.map( x =>s"${ x } --> ${ d.resDetail.id }_res")).flatten) = {
   val flowResult = result.flatMap{ x =>Seq(Some(RirfFlow("res", x.resDetail.id, x.resDetail.name)), x.appDetail.map( d =>RirfFlow("app", d.appDef.appInfo.id, d.appDef.appInfo.name))).flatten
}
 val flowObjects = flowResult.map{ x =>x.kind match {
         case "app" => s"[${ x.id }\\n${ x.name }] as ${ x.id }"
         case "res" => s"""artifact "${ x.id }\\n${ x.name }" as ${ x.id }_res"""
      }
}
 val flowLinks = result.flatMap{
      case d:RirfData => flowLinkFunc(d)
      case _ => Seq.empty[String]
      }.distinct
(flowObjects ++ flowLinks).mkString("\n")
   }

   def createFlowRenderWithRename(result: Seq[IrrfData], flowLinkFunc: RirfData => Seq[String] = d =>Seq(d.appDetail.map( x =>s"${ d.resDetail.id }_res --> ${ x.appDef.appInfo.id }"), d.parentAppId.map( x =>s"${ x } --> ${ d.resDetail.id }_res")).flatten) = {
   val flowResult = result.flatMap{ x => val beforeName = x.renameApps.headOption.map(_.beforeName).getOrElse("")
 val afterName = x.renameApps.lastOption.map(_.afterName).getOrElse("")
Seq(
Some(IrrfFlow(RirfFlow("res", x.appData.resDetail.id, x.appData.resDetail.name), "", "")), 
x.appData.appDetail.map( d =>IrrfFlow(RirfFlow("app", d.appDef.appInfo.id, d.appDef.appInfo.name), beforeName, afterName))).flatten
}
 val flowObjects = flowResult.map{ ir => val x = ir.rirfFlow
x.kind match {
         case "app"if (ir.beforeName == ir.afterName) => s"[${ x.id }\\n${ x.name }] as ${ x.id }"
         case "app"if (ir.beforeName != ir.afterName) => s"[${ x.id }\\n${ x.name }] as ${ x.id }\nnote right of ${ x.id } : ${ ir.beforeName } -> ${ ir.afterName }"
         case "res" => s"""artifact "${ x.id }\\n${ x.name }" as ${ x.id }_res"""
      }
}.distinct
 val flowLinks = result.flatMap{
      case d:IrrfData => flowLinkFunc(d.appData)
      case _ => Seq.empty[String]
      }.distinct
(flowObjects ++ flowLinks).mkString("\n")
   }

   def createReferResult(result: Seq[RirfData]) = {
   result.map{ x => val res = if (x.resDetail.path.isEmpty)
         {
         Seq(x.resDetail.id, x.resDetail.name)
         }
else
         {
         Seq(s"[${ x.resDetail.id }](${ x.resDetail.path })", x.resDetail.name)
         }
 val app = x.appDetail.map{ app =>if (app.path.isEmpty)
         {
         Seq(app.appDef.appInfo.id, app.appDef.appInfo.name)
         }
else
         {
         Seq(s"[${ app.appDef.appInfo.id }](${ app.path })", app.appDef.appInfo.name)
         }
}.getOrElse(Seq("-", "-"))
(res ++ app).mkString("| ", " | ", " |")
}.distinct.mkString("\n")
   }
}