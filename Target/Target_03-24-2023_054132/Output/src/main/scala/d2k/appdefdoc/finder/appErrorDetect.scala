package d2k.appdefdoc.finder

import Commons._
import scala.util.Try
import scala.reflect.io.Directory
import java.io.FileWriter
import org.apache.commons.io.output.FileWriterWithEncoding
import d2k.appdefdoc.parser._
import java.io.File
import d2k.appdefdoc.parser.D2kParser
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object AppErrorDetector extends App with D2kParser {
   val isLocalMode = args.size >= 4

   val (baseUrl, branch, targetName) = (args(0), args(1), args(2))

   val basePath = if (isLocalMode)
      args(3)
else
      "C:/d2k_docs"

   val writeBase = s"data/irFinder/${ targetName }"

   val writePath = Directory(writeBase)

   println(s"[Start App Relation Finder${ if (isLocalMode)
      " on Local Mode"
else
      "" }] ${ args.mkString(" ") }")

   def recList(file: File) : Array[File] = {
   val files = file.listFiles
files ++ files.filter(_.isDirectory).flatMap(recList)
   }

   //  val itemBasePath = s"${basePath}/apps/common"
   //  val itemNames = recList(new File(itemBasePath)).filter(_.getName.contains(".md"))
   //  val targetResouces = itemNames.filter { path =>
   //    Try { readItemDefMd(path.toString).contains(targetName) }.getOrElse({ println(s"itemDef parse error: ${path}"); false })
   //  }.toList
   //  println(targetResouces)
   //  val itemDefList = targetResouces.flatMap(x => Try((x.toString, ItemDefParser(x.toString).get)).toOption).toList
   //  println(itemDefList)
   //
   val appBasePath = s"${ basePath }/apps"

   var errList = List.empty[String]

   val appDefList = recList(new File (appBasePath)).filter(_.getName.contains("README.md")).flatMap( x =>Try((x.toString, AppDefParser(x.toString).get)).toOption.orElse({
   errList = x.toString :: errList;None
   }))

   println(errList.size)

   errList.reverse.foreach(println)
//
//  println(appDefList.head._1)
//  println(appDefList.head._2.appInfo)
//  println(appDefList.head._2.componentList)
/*
val result = appDefList.map { x =>
val (path, appdef) = x
val in = appdef.inputList.map(_.id).contains(targetName)
val out = appdef.outputList.map(_.id).contains(targetName)
val containType = (in, out) match {
case (false, false) => "none"
case (true, false)  => "in"
case (false, true)  => "out"
case (true, true)   => "io"
}
val ioData = appdef.inputList.filter(_.id == targetName).headOption.orElse(appdef.outputList.filter(_.id == targetName).headOption)
RrfData(path, appdef.appInfo, ioData, containType)
}.filter(_.containType != "none")
result.headOption.map { r =>
writePath.createDirectory(true, false)
val targetObj = r.ioData.get
val targetObjPath = s"${basePath}/apps/common/${targetObj.path.split("/common/")(1)}"
val targetObjTitle = s"[${result.head.ioData.get.id}](${targetObjPath})[${targetObj.name}]"
val targetObjUml = s"""artifact "${targetObj.id}\\n${targetObj.name}" as ${targetObj.id}"""
val appUml = result.map { d =>
s"[${d.appInfo.id}\\n${d.appInfo.name}] as ${d.appInfo.id}"
}
val chainUml = result.map { d =>
d.containType match {
case "in"  => s"${targetObj.id} --> ${d.appInfo.id} :Input"
case "out" => s"${d.appInfo.id} --> ${targetObj.id} :Output"
case "io"  => s"${targetObj.id} --> ${d.appInfo.id} :Input\\n${d.appInfo.id} --> ${targetObj.id} :Output"
case _     => ""
}
}
val umls = targetObjUml :: appUml ++ chainUml
def dataToTable(arf: RrfData) = s"| [${arf.appInfo.id}](${arf.path}) | ${arf.appInfo.name} |"
val outputTables = result.filter(_.containType == "out").map(dataToTable)
val inputTables = result.filter(_.containType == "in").map(dataToTable)
val tmpl = fileToStr("arfTemplates/searchResult.tmpl")
val writeFilePath = s"${writeBase}/${targetObj.id}.md"
val writer = new FileWriter(writeFilePath)
val conved = tmpl.replaceAllLiterally("%%SearchTarget%%", targetObjTitle)
.replaceAllLiterally("%%ResultPlantuml%%", umls.mkString("\n"))
.replaceAllLiterally("%%ResultOutput%%", outputTables.mkString("\n"))
.replaceAllLiterally("%%ResultInput%%", inputTables.mkString("\n"))
writer.write(conved)
writer.close
val csvData = result.map { arf =>
Seq(arf.appInfo.id, arf.appInfo.name, arf.containType).mkString(",")
}.mkString("\n")
val writeCsvFilePath = s"${writeBase}/${targetObj.id}.csv"
val csvWriter = new FileWriter(writeCsvFilePath)
csvWriter.write(csvData)
csvWriter.close
println(s"[Finish App Relation Finder] ${writeFilePath}")
}.getOrElse(println(s"[Finish App Relation Finder] Not Found Application"))
*
*/
}