package d2k.appdefdoc.finder

import Commons._

import scala.io.Source
import scala.util.Try
import scala.reflect.io.Directory
import java.io.FileWriter
import org.apache.commons.io.output.FileWriterWithEncoding
import d2k.appdefdoc.parser._
import java.io.File

case class RrfData(path: String, appInfo: AppInfo, ioData: Option[IoData], containType: String)
object ResourceRelationFinder extends App {
  val isLocalMode = args.size >= 4
  val (baseUrl, branch, targetName) = (args(0), args(1), args(2))
  val basePath = if (isLocalMode) args(3) else "C:/d2k_docs"
  val writePath = Directory(s"data/rrFinder/${targetName}")

  println(s"[Start App Relation Finder${if (isLocalMode) " on Local Mode" else ""}] ${args.mkString(" ")}")

  val appBasePath = s"${basePath}/apps"
  val result = createRrfData(targetName, appDefList(appBasePath))
  writeRrfData(targetName, baseUrl, basePath, writePath, Some("[Finish Resource Relation Finder]"), result)
}


