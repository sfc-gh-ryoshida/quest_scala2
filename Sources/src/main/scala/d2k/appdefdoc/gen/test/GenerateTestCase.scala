package d2k.appdefdoc.gen.test

import d2k.common.MakeResource
import scala.io.Source
import scala.reflect.io.Path
import scala.reflect.io.Path.string2path

case class IoMdInfo(ioType: String, id: String, name: String, path: String)
case class OutputData(
  groupId: String, appId: String, testCase: String, inputMdData: Seq[(IoMdInfo, String)], outputMdData: Seq[(IoMdInfo, String)]) {
  def write(outputBasePath: String = s"data/testGen") = {
    writeTestCase(outputBasePath)
    writeInputMd(outputBasePath)
    writeOutputMd(outputBasePath)
  }
  def writeTestCase(outputBasePath: String) = {
    val writePath = Path(s"${outputBasePath}/${groupId}")
    writePath.createDirectory(failIfExists = false)
    val outPath = writePath / s"${appId}Test.scala"
    outPath.toFile.writeAll(testCase)
  }

  def writeInputMd(outputBasePath: String) = {
    val writePath = Path(s"${outputBasePath}/markdown/${appId}/AT")
    writePath.createDirectory(failIfExists = false)
    inputMdData.foreach {
      case (ioMd, tableData) =>
        val outPath = writePath / s"${ioMd.id}.md"
        val outputData = s"# ${ioMd.name}\n${tableData}"
        outPath.toFile.writeAll(tableData)
    }
  }

  def writeOutputMd(outputBasePath: String) = {
    val writePath = Path(s"${outputBasePath}/markdown/${appId}/AT")
    writePath.createDirectory(failIfExists = false)
    outputMdData.foreach {
      case (ioMd, tableData) =>
        val outPath = writePath / s"${ioMd.id}.md"
        val outputData = s"# ${ioMd.name}\n${tableData}"
        outPath.toFile.writeAll(tableData)
    }
  }
}

object GenerateTestCase {
  def apply(baseUrl: String) = new GenerateTestCase(baseUrl)
}

class GenerateTestCase(baseUrl: String) {
  def generate(branch: String, appGroup: String, appId: String) = {
    val appBaseUrl = s"${baseUrl}/raw/${branch}/apps/${appGroup}/${appId}"
    val itemsBaseUrl = s"${baseUrl}/raw/${branch}/apps/common/items"
    val url = s"${appBaseUrl}/README.md"

    val md = Source.fromURL(s"${url}?private_token=${sys.env("GITLAB_TOKEN")}").getLines.toList
    val ioList = md.filter(!_.isEmpty).dropWhile(line => !line.contains("## 03. 入出力データ一覧"))
    val inputList = ioList.drop(2).takeWhile(!_.contains("### 03.02. 出力")).drop(2)
    val outputList = ioList.dropWhile(!_.contains("### 03.02. 出力")).drop(1)

    def strToIoMdInfo(str: String) = {
      val ioInfoRegx = "\\|\\s*\\[(.*)]\\((.*)\\)\\s*\\|(.*)\\|(.*)\\|".r
      ioInfoRegx.findFirstMatchIn(str)
        .map(g => IoMdInfo(g.group(3).trim, g.group(1).trim, g.group(4).trim, g.group(2).trim))
    }

    val ioTypeToCnvMethodName = (ioType: String, appId: String) => ioType.toLowerCase match {
      case "pq"          => s"""toPq("${appId}")"""
      case "db"          => s"""toDb("${appId}")"""
      case "jef"         => s"""toJef("${appId}")"""
      case "file(fixed)" => "toFixed(\"writePath\")"
      case "file(csv)"   => "toCsv(\"writePath\")"
      case "file(tsv)"   => "toTsv(\"writePath\")"
    }

    val ioTypeToCheckMethodName = (ioType: String, appId: String) => ioType.toLowerCase match {
      case "pq"          => s"""checkPq("${appId}.pq")"""
      case "db"          => s"""checkDb("${appId}")"""
      case "file(fixed)" => "checkFixed(\"writePath\")"
      case "file(csv)"   => "checkCsv(\"writePath\")"
      case "file(tsv)"   => "checkTsv(\"writePath\")"
    }

    val tableTemplate = "        //%%inputDataName%%\n        %%inputData%%.%%inputConvMethod%%"
    def imiToTemplate(imi: IoMdInfo, ioTypeCnv: (String, String) => String) = {
      val itemName = imi.path.split("/").takeRight(2).mkString("/")
      tableTemplate
        .replaceAllLiterally("%%inputDataName%%", imi.name)
        .replaceAllLiterally("%%inputData%%", s"""x.readMdTable("${imi.id}.md")""")
        .replaceAllLiterally("%%inputConvMethod%%", ioTypeCnv(imi.ioType, imi.id))
    }

    def imiToMdData(imi: IoMdInfo) = {
      println(s"read:${imi.id}:${imi.name}")
      val itemName = imi.path.split("/").takeRight(3).mkString("/")
      s"# ${imi.name}\n${MakeResource.itemsMdToTable(s"${itemsBaseUrl}/${itemName}").getOrElse("")}\n"
    }

    val template = Option(getClass.getClassLoader.getResourceAsStream("genTemplates/testcaseAt.tmpl"))
      .map(is => Source.fromInputStream(is)).get.mkString
    val inputInfos = inputList.flatMap(strToIoMdInfo).map(d => imiToTemplate(d, ioTypeToCnvMethodName))
    val inputMdData = inputList.flatMap(strToIoMdInfo).map(d => (d, imiToMdData(d)))
    val outputInfos = outputList.flatMap(strToIoMdInfo).map(d => imiToTemplate(d, ioTypeToCheckMethodName))
    val outputMdData = outputList.flatMap(strToIoMdInfo).map(d => (d, imiToMdData(d)))
    val testCaseStr = template.replaceAllLiterally("%%APP_NAME%%", appId)
      .replaceAllLiterally("%%PROJECT_ID%%", appGroup)
      .replaceAllLiterally("%%READ_DATA%%", inputInfos.mkString("\n\n"))
      .replaceAllLiterally("%%CHECK_DATA%%", outputInfos.mkString("\n\n"))

    OutputData(appGroup, appId, testCaseStr, inputMdData, outputMdData)
  }
}
