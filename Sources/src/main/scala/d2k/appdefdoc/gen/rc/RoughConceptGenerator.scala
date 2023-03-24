package d2k.appdefdoc.gen.rc

import scala.io.Source
import scala.util.Try
import scala.reflect.io.Directory
import java.io.FileWriter
import org.apache.commons.io.output.FileWriterWithEncoding
import d2k.appdefdoc.parser._
import java.io.File

object RCData {
  private val strToPlantUml = (obj: String) => {
    obj.split("_")(1).split("To")
  }

  private def splitIdAndName(s: String) = {
    val splitted = s.split('[')
    (splitted(0).trim, splitted(1).trim.dropRight(1))
  }
  private val inputTypeToPlantUml =
    (appId: String, indata: (Cf, ComponentDetail)) => {
      val (cf, cd) = indata
      strToPlantUml(cf.componentId)(0) match {
        case "Pq" => {
          val (id, name) = splitIdAndName(cd.findById("01").value)
          (s"""artifact "${id}\\n${name}" as ${id}""", s"""[抽出] as ${cf.componentId}""", s"${id} --> ${cf.componentId}")
        }
        case "File" => {
          val (id, name) = splitIdAndName(cd.findById("02", "02.02").value.mkString)
          (s"""artifact "${id}\\n${name}" as ${id}""", s"""[抽出] as ${cf.componentId}""", s"${id} --> ${cf.componentId}")
        }
        case "Db" => {
          val dbInfo = cd.processes.map(_.detail.filter(_.param.id == "02").head.values.head.asInstanceOf[ProcessParamValue].value).head
          val (id, name) = splitIdAndName(cd.findById("02").value)
          (s"""database "${id}\\n${name}" as ${id}""", s"""[抽出] as ${cf.componentId}""", s"${id} --> ${cf.componentId}")
        }
        case "Df"     => ("", s"""[編集] as ${cf.componentId}""", "")
        case "DfJoin" => ("", s"""[結合] as ${cf.componentId}""", "")
        case _        => ("", "", "")
      }
    }

  private val outputTypeToPlantUml =
    (appId: String, indata: (Cf, ComponentDetail)) => {
      val (cf, cd) = indata
      strToPlantUml(cf.componentId)(1) match {
        case "Pq" => {
          val pqInfo = cd.processes.map(_.detail.filter(_.param.id == "01").head.values.head.asInstanceOf[ProcessParamValue].value).head
          val (id, name) = splitIdAndName(pqInfo)
          (s"""artifact "${id}\\n${name}" as ${id}""", s"${cf.componentId} --> ${id}")
        }
        case "File" => {
          val fileInfo = cd.processes.map(_.detail.filter(_.param.id == "02").head.values.flatMap {
            case x: ProcessParamSubDetail => Some(x)
            case _                        => None
          }.filter(_.id == "02.02.").head.value).head
          val (id, name) = splitIdAndName(fileInfo.mkString)
          (s"""artifact "${id}\\n${name}" as ${id}""", s"${cf.componentId} --> ${id}")
        }
        case "Db" => {
          val dbInfo = cd.processes.map(_.detail.filter(_.param.id == "02").head.values.head.asInstanceOf[ProcessParamValue].value).head
          val (id, name) = splitIdAndName(dbInfo)
          (s"""database "${id}\\n${name}" as ${id}""", s"${cf.componentId} --> ${id}")
        }
        case _ => ("", "")
      }
    }

  def apply(appdef: AppDef)(indata: (Cf, ComponentDetail)) = {
    val in = inputTypeToPlantUml(appdef.appInfo.id, indata)
    val out = outputTypeToPlantUml(appdef.appInfo.id, indata)
    Seq(new RCData(in._1, in._2, in._3), new RCData(out._1, "", out._2))
  }
}

case class RCData(obj: String, frame: String, link: String)

case class RCDataStore(objs: List[String] = List.empty[String], frames: List[String] = List.empty[String], links: List[String] = List.empty[String]) {
  def +=(rcd: RCData) = this.copy(
    if (rcd.obj.isEmpty) objs else rcd.obj :: objs,
    if (rcd.frame.isEmpty) frames else rcd.frame :: frames,
    if (rcd.link.isEmpty) links else rcd.link :: links)
  def result = RCDataStore(objs.reverse, frames.reverse, links.reverse)
}

object RoughConceptGenerator extends App {
  val isLocalMode = args.size >= 5
  val (baseUrl, branch, appGroup, appId) = (args(0), args(1), args(2), args(3))
  val basePath = if (isLocalMode) args(4) else ""
  val writeBase = s"data/rcGen/${appGroup}/${appId}"
  val writePath = Directory(writeBase)
  writePath.createDirectory(true, false)

  println(s"[Start Rough Concept Generate${if (isLocalMode) " on Local Mode" else ""}] ${args.mkString(" ")}")
  val appdef = if (isLocalMode) {
    AppDefParser(basePath, appGroup, appId).get
  } else {
    AppDefParser(baseUrl, branch, appGroup, appId).get
  }
  val appdefStr = generate(appdef)

  val writeFilePath = s"${writeBase}/README.md"
  val writer = new FileWriter(writeFilePath)
  writer.write(appdefStr)
  writer.close

  val psFilePath = s"${writeBase}/ps.puml"
  val fileCheck = new File(psFilePath).exists
  if (!fileCheck) new FileWriter(psFilePath).close

  println(s"[Finish Rough Concept Generate] ${writeFilePath}")

  def fileToStr(fileName: String) =
    Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName)).mkString

  def ioToTableStr(iodata: IoData) = Seq(
    s"[${iodata.id}](${iodata.path})",
    iodata.srcType,
    iodata.name).mkString("| ", " | ", " |")

  def generate(appdef: AppDef) = {
    val componentdefs = appdef.componentList.map { comp =>
      println(s"Parsing ${comp.id}[${comp.name}]")
      if (isLocalMode) {
        ComponentDefParser(basePath, appGroup, appId, comp.mdName).get
      } else {
        ComponentDefParser(baseUrl, branch, appGroup, appId, comp.mdName).get
      }
    }

    val componentFlow = appdef.componentFlow
    val rcd = RCData(appdef) _
    val objects =
      appdef.componentFlow.get.pairs.flatMap(x => Seq(x.cf1, x.cf2))
        .flatMap {
          case cf: Cf => {
            val targetComponent = componentdefs.seq.filter(_.componentInfo.id == cf.componentId)
            Some((cf, targetComponent.head.componentDetail))
          }
          case _ => None
        }.sortBy(_._1.componentId).flatMap(rcd)

    val links = appdef.componentFlow.get.pairs.flatMap { pair =>
      pair.cf2 match {
        case CfEnd => None
        case _ =>
          Some(s"${pair.cf1.componentId} --> ${pair.cf2.componentId}")
      }
    }
    val rcds = objects.foldLeft(RCDataStore()) { (l, r) => l += r }.result
    def objToMd(rcds: RCDataStore) = {
      s"""${rcds.objs.mkString("\n")}
      
frame ${appdef.appInfo.id} {
${rcds.frames.mkString("\n")}
}

${(rcds.links ++ links).mkString("\n")}

!include ps.puml"""
    }
    val rcMdTmpl = fileToStr("genTemplates/rc/AppMd.tmpl")
    Seq(
      ("%%AppId%%", appdef.appInfo.id),
      ("%%AppName%%", appdef.appInfo.name),
      ("%%AppDesc%%", appdef.appInfo.desc),
      ("%%PlantUml%%", objToMd(rcds)),
      ("%%IoInput%%", appdef.inputList.map(ioToTableStr).mkString("\n")),
      ("%%IoOutput%%", appdef.outputList.map(ioToTableStr).mkString("\n")))
      .foldLeft(rcMdTmpl) { (l, r) => l.replaceAllLiterally(r._1, r._2) }
  }
}
