package d2k.common.df

import d2k.common.InputArgs
import d2k.common.ResourceInfo
import d2k.common.fileConv.FileConv
import scala.util.Try

trait ReadFile extends ResourceInfo {
  val fileInputInfo: FileInputInfoBase
  lazy val itemConfId: String = componentId
  def readFile(implicit inArgs: InputArgs) = {
    new FileConv(componentId, fileInputInfo, itemConfId).makeDf
  }
}
