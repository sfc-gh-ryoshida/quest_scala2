package d2k.appdefdoc.gen.src

import scala.io.Source
import scala.util.Try
import scala.reflect.io.Directory
import java.io.FileWriter
import ItemReplace._
import ItemReplace.scala._
import org.apache.commons.io.output.FileWriterWithEncoding
import d2k.appdefdoc.parser._

object SourceGenerator extends App {
  val isLocalMode = args.size >= 5
  val (baseUrl, branch, appGroup, appId) = (args(0), args(1), args(2), args(3))
  val basePath = if (isLocalMode) args(4) else ""
  val writeBase = s"data/srcGen/${appGroup}"
  val writePath = Directory(writeBase)
  writePath.createDirectory(true, false)

  println(s"[Start Source Generate${if (isLocalMode) " on Local Mode" else ""}] ${args.mkString(" ")}")
  val appdef = if (isLocalMode) {
    AppDefParser(basePath, appGroup, appId).get
  } else {
    AppDefParser(baseUrl, branch, appGroup, appId).get
  }
  val appdefStr = generate(appdef)

  val writeFilePath = s"${writeBase}/${appId}.scala"
  val writer = new FileWriter(writeFilePath)
  writer.write(appdefStr)
  writer.close

  val inputFiles = appdef.inputList.filter(_.srcType.toLowerCase.startsWith("file"))
  inputFiles.headOption.foreach { _ =>
    generateItemConf(baseUrl, branch, appGroup, appId, writePath)(inputFiles)
  }

  println(s"[Finish Source Generate] ${writeFilePath}")

  def fileToStr(fileName: String) =
    Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName)).mkString

  def generate(appdef: AppDef) = {
    val componentdefs = appdef.componentList.map { comp =>
      println(s"Parsing ${comp.id}[${comp.name}]")
      if (isLocalMode) {
        ComponentDefParser(basePath, appGroup, appId, comp.mdName).get
      } else {
        ComponentDefParser(baseUrl, branch, appGroup, appId, comp.mdName).get
      }
    }

    val componentTmpl = fileToStr("genTemplates/component.tmpl")
    val componentStr = componentdefs.map { c =>
      println(s"Generating ${c.componentInfo.id}[${c.componentInfo.name}]")
      val conved = c.componentDetail.processes.flatMap { proc =>
        proc.detail.flatMap { x =>
          x.values.flatMap {
            case ProcessParamSubDetail(id, name, defaultCode, value) =>
              Some((defaultCode, value))
            case _ => None
          }.groupBy(_._1).map { b =>
            ProcessDetail(x.param, Some(b._1), b._2.map(x => ProcessParamValue(x._2.mkString)))
          }
        }
      }

      val componentDefs = c.componentDetail.processes.flatMap(
        _.detail.flatMap(p => ConvertTemplateDefine.modDetail(baseUrl, branch)(c.componentDetail, p)))
      val implimentTmpl = fileToStr("genTemplates/impliment.tmpl")
      val funcIds = c.implimentLogic.impls.flatMap {
        case Impliment関数定義(id, desc, args, impls)    => None
        case ImplimentUDF関数定義(id, desc, args, impls) => None
        case x: Impliment関数呼出                        => Some(x.genSrc(""))
        case x                                       => Some(x.id)
      }.mkString(" ~> ")

      val implStr = c.implimentLogic.impls.map {
        case x: Impliment全体編集     => x.genSrc(fileToStr("genTemplates/impl全体編集.tmpl"))
        case x: Impliment部分編集     => x.genSrc(fileToStr("genTemplates/impl部分編集.tmpl"))
        case x: Implimentキャスト     => x.genSrc(fileToStr("genTemplates/implキャスト.tmpl"))
        case x: Impliment選択       => x.genSrc(fileToStr("genTemplates/impl選択.tmpl"))
        case x: Impliment抽出       => x.genSrc(fileToStr("genTemplates/impl抽出.tmpl"))
        case x: Impliment出力項目並び替え => x.copy(baseUrl = baseUrl, branch = branch).genSrc(fileToStr("genTemplates/impl出力項目並び替え.tmpl"))
        case x: Implimentグループ抽出   => x.genSrc(fileToStr("genTemplates/implグループ抽出.tmpl"))
        case x: Impliment集計       => x.genSrc(fileToStr("genTemplates/impl集計.tmpl"))
        case x: Impliment再分割      => x.genSrc(fileToStr("genTemplates/impl再分割.tmpl"))
        case x: Implimentキャッシュ    => x.genSrc(fileToStr("genTemplates/implキャッシュ.tmpl"))
        case x: Implimentキャッシュ解放  => x.genSrc(fileToStr("genTemplates/implキャッシュ解放.tmpl"))
        case x: Impliment関数定義     => x.genSrc(fileToStr("genTemplates/impl関数定義.tmpl"))
        case x: Impliment関数呼出     => ""
        case x: ImplimentUDF関数定義  => x.genSrc(fileToStr("genTemplates/implUDF関数定義.tmpl"))
        case x: ImplimentUDF関数適用  => x.genSrc(fileToStr("genTemplates/implUDF関数適用.tmpl"))
      }.filter(!_.isEmpty).mkString("\n\n")

      val impliments = implimentTmpl
        .replaceAllLiterally("%%functions%%", funcIds)
        .replaceAllLiterally("%%impliments%%", implStr)

      val componentRepList = Seq(
        ("%%componentDesc%%", s"${c.componentInfo.id} ${c.componentInfo.name}"),
        ("%%componentName%%", s"c${c.componentInfo.id}"),
        ("%%templateId%%", c.componentInfo.id.split("_").toList.last),
        ("%%executor%%", if (c.implimentLogic.impls.isEmpty) "Nothing" else "Executor"),
        ("%%componentId%%", addDq(c.componentDetail.componentId)),
        ("%%procImpliments%%", componentDefs.mkString("\n")),
        ("%%impliments%%", if (c.implimentLogic.impls.isEmpty) "" else impliments))
      componentRepList.foldLeft(componentTmpl) { (l, r) =>
        l.replaceAllLiterally(r._1, r._2)
      }
    }.mkString("\n\n")

    def defaultAppFlow = appdef.componentList.map {
      case x if !x.id.contains("_Df") => s"c${x.id}.run(Unit)"
      case x                          => s"c${x.id}.run"
    }.mkString(" ~>\n    ")

    val flowLogic = appdef.componentFlow.map { cf =>
      val flowPair = cf.pairs.map(p => (p.cf1.componentId, p.cf2.componentId))
      FlowLogicGenerator(flowPair)
    }.getOrElse(defaultAppFlow)

    val mainTmpl = fileToStr("genTemplates/main.tmpl")
    val mainRepList = Seq(
      ("%%appDesc%%", appdef.appInfo.desc),
      ("%%appId%%", appdef.appInfo.id),
      ("%%appFlow%%", flowLogic),
      ("%%components%%", componentStr))
    mainRepList.foldLeft(mainTmpl) { (l, r) =>
      l.replaceAllLiterally(r._1, r._2)
    }
  }

  def generateItemConf(baseUrl: String, branch: String, appGroup: String, appId: String, writePath: Directory)(inputFiles: Seq[IoData]) = {
    val fileHeader = Seq("itemId", "itemName", "length", "cnvType", "extractTarget", "comment").mkString("\t")
    val writeConfPath = Directory(s"${writePath}/itemConf")
    writeConfPath.createDirectory(true, false)

    inputFiles.map { iodata =>
      println(s"  Parsing ${iodata.id}[${iodata.name}](${iodata.path})")

      val filePath = iodata.path.split('/').takeRight(2).mkString("/")
      val itemdef = ItemDefParser(baseUrl, branch, filePath).get
      val itemDetails = itemdef.details.map { d =>
        Seq(d.id, d.name, d.size, d.dataType, "false", "").mkString("\t")
      }
      val outputList = fileHeader :: itemDetails

      val writeFilePath = s"${writeConfPath}/${appGroup}_items_${appId}_${iodata.id}.conf"
      val writer = new FileWriterWithEncoding(writeFilePath, "MS932")
      writer.write(outputList.mkString("\r\n"))
      writer.write("\r\n")
      writer.close
      println(s"  [generate itemConf] ${writeFilePath.replaceAllLiterally("\\", "/")}")
    }
  }
}
