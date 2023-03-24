package d2k.appdefdoc.parser

import scala.io.Source

trait D2kParser {
  def readAppDefMd(baseUrl: String, branch: String, appGroup: String, appId: String, fileName: String) = {
    val appBaseUrl = s"${baseUrl}/raw/${branch}/apps/${appGroup}/${appId}"
    val url = s"${appBaseUrl}/${fileName}"
    Source.fromURL(s"${url}?private_token=${sys.env("GITLAB_TOKEN")}").getLines.mkString("\n")
  }

  def readItemDefMd(baseUrl: String, branch: String, filePath: String) = {
    val itemsBaseUrl = s"${baseUrl}/raw/${branch}/apps/common/items"
    val url = s"${itemsBaseUrl}/${filePath}"
    Source.fromURL(s"${url}?private_token=${sys.env("GITLAB_TOKEN")}").getLines.mkString("\n")
  }

  def readAppDefMd(basePath: String, appGroup: String, appId: String, fileName: String) = {
    val appBasePath = s"${basePath}/apps/${appGroup}/${appId}"
    val path = s"${appBasePath}/${fileName}"
    Source.fromFile(path).getLines.mkString("\n")
  }

  def readAppDefMd(basePath: String) = {
    Source.fromFile(basePath).getLines.mkString("\n")
  }

  def readItemDefMd(basePath: String) = {
    Source.fromFile(basePath).getLines.mkString("\n")
  }
}