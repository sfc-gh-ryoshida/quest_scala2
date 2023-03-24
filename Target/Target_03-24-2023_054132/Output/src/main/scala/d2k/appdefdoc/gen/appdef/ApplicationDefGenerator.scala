package d2k.appdefdoc.gen.appdef

import scala.io.Source
import scala.util.Try
import scala.reflect.io.Directory
import java.io.FileWriter
import org.apache.commons.io.output.FileWriterWithEncoding
import d2k.appdefdoc.parser._
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class InputData (id: String, cType: String, desc: String)
 object ApplicationDefGenerator extends App {
val (grpId, appId, inputCsv) = (args(0), args(1), if (args.size >= 3)
      args(2)
else
      "./data/input.csv")
println(s"[Start ApplicationDef Generate] ${ Seq(grpId, appId, inputCsv).mkString(" ") }")
 val adg = new ApplicationDefGenerator (grpId, appId, inputCsv)
 val result = adg.makeAppDef
adg.makeComponentDef
println(s"[Finish ApplicationDef Generate] ${ result }")
 def searchAndReplace(target: Seq[String], searchElem: String, replaceElem: String) = {
   val idx = target.indexWhere(_.contains(searchElem))
target.updated(idx, replaceElem)
   }
 def fileToStr(fileName: String) = Source.fromFile(fileName)
 def resToStr(fileName: String) = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName)).mkString
}
 class ApplicationDefGenerator (grpId: String, appId: String, inputCsv: String) {
   import ApplicationDefGenerator._

   val writeBase = s"data/appGen/${ grpId }/${ appId }"

   val writePath = Directory(writeBase)

   writePath.createDirectory(true, false)

   val indata = fileToStr(inputCsv).getLines.zipWithIndex.map{
   case (x, idx) => val splitted = x.split(',')
InputData(f"${ idx + 1 }%02d_${ splitted(0) }", splitted(0), splitted(1))
   }.toSeq

   def makeAppDef = {
   val appPath = "./catalogs/01_application/README.md"
 val appMd = fileToStr(appPath).getLines.toSeq
 val componentList = indata.map{ x =>s"| [${ x.id }](${ x.id }.md) | ${ x.desc } |"
}
 val componentFlow = indata.sliding(2).map{ x =>if (x.size == 1)
         {
         (x(0), x(0))
         }
else
         {
         (x(0), x(1))
         }
}.toSeq
 val componentFlowStr = if (componentFlow.size == 1)
         {
         componentFlow.take(1).map{ x =>s"""${ x._2.id } --> (*)"""
}
         }
else
         {
         componentFlow.take(1).map{ x =>s""""${ x._1.id }\\n${ x._1.desc }" as ${ x._1.id } --> "${ x._2.id }\\n${ x._2.desc }" as ${ x._2.id }"""
} ++ componentFlow.drop(1).map{ x =>s"""${ x._1.id } --> "${ x._2.id }\\n${ x._2.desc }" as ${ x._2.id }"""
} ++ componentFlow.takeRight(1).map{ x =>s"""${ x._2.id } --> (*)"""
}
         }
 val appdefStr = Seq(
("## %%AppId%%", s"## ${ appId }"), 
("%%[01_XxToXx](01_XxToXx.md)%%", componentList.mkString("\n")), 
("%%01_XxToXx%%", componentFlowStr.mkString("\n"))).foldLeft(appMd){(l, r) =>searchAndReplace(l, r._1, r._2)
}.mkString("\n")
 val writeFilePath = s"${ writeBase }/README.md"
 val writer = new FileWriter (writeFilePath)
writer.write(appdefStr)
writer.close
writeFilePath
   }

   def makeComponentDef = {
   val catalogPath = "./catalogs/02_templates"
indata.foreach{ x => val cMd = fileToStr(s"${ catalogPath }/_${ x.cType }.md").getLines.toSeq
 val cdefStr = Seq(
(s"## _${ x.cType }", s"## ${ x.id }"), 
("%%コンポーネント名%%", x.desc), 
("%%コンポーネントID%%", s"    ${ appId }${ x.id.take(2) }")).foldLeft(cMd){(l, r) =>searchAndReplace(l, r._1, r._2)
}.mkString("\n")
 val writeFilePath = s"${ writeBase }/${ x.id }.md"
 val writer = new FileWriter (writeFilePath)
writer.write(cdefStr)
writer.close
}
   }
}