package d2k.appdefdoc.finder.jsonbase

import d2k.appdefdoc.finder._
import d2k.appdefdoc.finder.{ Commons => fcom }
import Commons._

import scala.io.Source
import scala.util.Try
import scala.reflect.io.Directory
import java.io.FileWriter
import org.apache.commons.io.output.FileWriterWithEncoding
import d2k.appdefdoc.parser._
import java.io.File
import scala.reflect.io.Path

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats

object ResourceRelationFinder extends App {
  val isLocalMode = args.size >= 4
  val (baseUrl, branch, targetName) = (args(0), args(1), args(2))
  val basePath = if (isLocalMode) args(3) else "C:/d2k_docs"
  val jsonPath = createJsonPath(basePath)

  println(s"[Start App Relation Finder using json data${if (isLocalMode) " on Local Mode" else ""}] ${args.mkString(" ")}")

  val linkReadJson = createLinkReadJson(jsonPath)
  val linkWriteJson = createLinkWriteJson(jsonPath)

  val writePath = Directory(s"data/js/rrFinder/${targetName}")

  val nodeAppMap = createNodeAppMap(jsonPath)
  val nodeResourceMap = createNodeResourceMap(jsonPath)

  val itemNamePath = createItemNamePathList(basePath)

  val jsonAppdef = createJsonAppdef(basePath, itemNamePath, nodeAppMap, nodeResourceMap, (linkReadJson ++ linkWriteJson).toList)
  val appBasePath = s"${basePath}/apps"
  val result = fcom.createRrfData(targetName, jsonAppdef.toSeq)
  fcom.writeRrfData(targetName, baseUrl, basePath, writePath, Some("[Finish App Relation Finder using json data]"), result)
}


