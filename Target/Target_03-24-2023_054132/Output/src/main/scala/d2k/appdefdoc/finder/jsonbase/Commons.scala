package d2k.appdefdoc.finder.jsonbase

import scala.reflect.io.Path
import scala.io.Source
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import d2k.appdefdoc.finder.{Commons => fcom}
import java.io.File
import scala.util.Try
import d2k.appdefdoc.parser.AppDefParser
import d2k.appdefdoc.parser.IoData
import d2k.appdefdoc.parser.AppDef
import d2k.appdefdoc.parser.AppInfo
import d2k.appdefdoc.parser.ComponentDefInfo
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class LinkData (appId: String, resourceId: String, dataType: String)
case class NodeData (logical_name: String, physical_name: String, dataType: String)
 object Commons {
   def createJsonPath(basePath: String) = Path(basePath).parent / "concept_flow" / "json"

   def jsStr(jv: JValue)(name: String) = (jv \ name).values.toString

   implicit val formats = DefaultFormats

   def createLinkReadJson(jsonPath: Path) = {
   val linkReadPath = (jsonPath / "link_read.json").jfile
Source.fromFile(linkReadPath, "MS932").getLines.map{ str => val value = jsStr(parse(str))_
LinkData(value("to_node"), value("from_node"), "input")
}
   }

   def createLinkWriteJson(jsonPath: Path) = {
   val linkWritePath = (jsonPath / "link_write.json").jfile
Source.fromFile(linkWritePath, "MS932").getLines.map{ str => val value = jsStr(parse(str))_
LinkData(value("from_node"), value("to_node"), "output")
}
   }

   def createNodeAppMap(jsonPath: Path) = {
   val nodeAppPath = (jsonPath / "node_application.json").jfile
Source.fromFile(nodeAppPath, "MS932").getLines.map{ str => val value = jsStr(parse(str))_
(value("physical_name"), NodeData(value("logical_name"), value("physical_name"), value("language")))
}.toMap
   }

   def createNodeResourceMap(jsonPath: Path) = {
   val nodeResourcePath = (jsonPath / "node_io.json").jfile
Source.fromFile(nodeResourcePath, "MS932").getLines.map{ str => val value = jsStr(parse(str))_
(value("key"), NodeData(value("logical_name"), value("physical_name"), value("data_type")))
}.toMap
   }

   def appDefList(appBasePath: String) = {
   fcom.recList(new File (appBasePath)).filter(_.getName.contains("README.md")).filter( x =>fcom.appDefRegx.findFirstMatchIn(x.toString).isDefined).flatMap( x =>Try{
      val appdef = AppDefParser(x.toString).get
(appdef.appInfo.id, x.toString)
      }.toOption.orElse{
      println(s"  appDef parse error: ${ x.toString }");None
      }).toList
   }

   def createIoData(itemNamePath: Array[(String, String)], nodeResourceMap: Map[String, NodeData])(data: Option[List[LinkData]]) = {
   data.map{
_.map{ ld => val path = itemNamePath.find{ x =>ld.resourceId.contains(x._1)
}.map(_._2).getOrElse("")
IoData(
nodeResourceMap.get(ld.resourceId).map(_.physical_name).getOrElse(""), 
path, 
nodeResourceMap.get(ld.resourceId).map(_.dataType).getOrElse(""), 
nodeResourceMap.get(ld.resourceId).map(_.logical_name).getOrElse(""))
}
}.getOrElse(List.empty[IoData])
   }

   def createItemNamePathList(basePath: String) = {
   val itemBasePath = s"${ basePath }/apps/common"
 val itemNames = fcom.recList(new File (itemBasePath)).filter(_.getName.contains(".md"))
itemNames.map( x =>(Path(x).name.dropRight(3), Path(x).toString.replaceAllLiterally("\\", "/")))
   }

   def removePhyphen(s: String) = s.replaceAllLiterally("-", "")

   def createJsonAppdef(basePath: String, itemNamePath: Array[(String, String)], nodeAppMap: Map[String, NodeData], nodeResourceMap: Map[String, NodeData], linkJson: List[LinkData]) = {
   val appDefResult = appDefList(s"${ basePath }/apps").toMap
 val creIoData = createIoData(itemNamePath, nodeResourceMap)_
linkJson.toList.groupBy(_.appId).mapValues(_.groupBy(_.dataType)).map{
      case (k, v) => AppDef(AppInfo(removePhyphen(k), nodeAppMap.get(k).map(_.logical_name).getOrElse(""), ""), None, 
List.empty[ComponentDefInfo], creIoData(v.get("input")), creIoData(v.get("output")))
      }.map( appdef =>(appDefResult.get(appdef.appInfo.id).getOrElse(""), appdef))
   }

   def mergeAppdef(jsonAppDef: Seq[(String, AppDef)], appDef: Seq[(String, AppDef)]) = jsonAppDef.map{ jAppdef =>appDef.find{ x =>jAppdef._2.appInfo.id.contains(x._2.appInfo.id)}.getOrElse(jAppdef)
}
}