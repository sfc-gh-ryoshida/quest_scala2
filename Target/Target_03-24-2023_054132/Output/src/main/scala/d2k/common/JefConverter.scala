package d2k.common

import scala.io.Source
import scala.util.Try
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object JefConverter {
   object implicits {
      implicit class JefConvert (s: String) {
         def toJefHalf : Array[Byte] = convUtfToJefHalf(s)

         def toJefFull : Array[Byte] = convUtfToJefFull(s)
      }
   }

   object controlCodes {
      val nullCode = "00"

      val tab = "05"

      val kanjiOut = "28"

      val kanjiIn9 = "29"

      val kanjiIn12 = "38"
   }

   def readData(fileName: String) = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName)).getLines.map{ line => val kv = line.split('\t')
(kv(0), kv(1))
}.toMap

   import controlCodes._

   def addControlCode(origin: Map[String, String]) = origin ++ Map(tab -> "\t", kanjiOut -> "", kanjiIn9 -> "", kanjiIn12 -> "")

   lazy val jefToUtfHalfData = addControlCode(readData("jefUtf8ConvHalf.tsv"))

   lazy val jefToUtfFullData = readData("jefUtf8ConvFull.tsv")

   lazy val jefToUtfFullKddiData = readData("jefUtf8ConvFull_kddi.tsv")

   lazy val utfToJefHalfData = (jefToUtfHalfData - nullCode - kanjiOut - kanjiIn9 - kanjiIn12).toSeq.map( x =>(x._2.toCharArray.headOption.getOrElse(""), x._1)).toMap

   lazy val utfToJefFullData = jefToUtfFullData.toSeq.map( x =>(x._2.toCharArray.head, x._1)).toMap

   def isJefHalf(domain: String, charEnc: String) = charEnc == "JEF" && !domain.startsWith("全角文字列")

   def isJefFull(domain: String, charEnc: String) = charEnc == "JEF" && domain.startsWith("全角文字列")

   def convJefToUtfHalf(data: Array[Byte]) : String = {
   val conved = data.map{ byte =>(byte, Try(jefToUtfHalfData(f"$byte%02X")))}
if (!conved.map(_._1).forall(_ == 0x00))
         printErrorHalf(conved)
conved.map(_._2.getOrElse("*")).mkString
   }

   private [this] def printErrorHalf(data: Array[(Byte, Try[String])]) = data.foreach{
   case (org, conv) => if (conv.isFailure == true)
      println(f"!!!![JEF CONV ERROR:HALF]$org%02X")
   }

   def convJefToUtfFull(data: Array[Byte]) : String = {
   data.grouped(2).map{ byteArr => val jefCode = byteArr.map( x =>f"$x%02X").mkString
jefToUtfFullKddiData.get(jefCode).orElse{
jefToUtfFullData.get(jefCode)
}.getOrElse{
      println(s"!!!![JEF CONV ERROR:FULL]${ byteArr.map( x =>f"$x%02X").mkString }")
"■"
      }
}.mkString
   }

   def convUtfToJefHalf(data: String) : Array[Byte] = data.map( x =>Try(utfToJefHalfData(x)).getOrElse("5C")).map( x =>Integer.parseInt(x.toString, 16)).map(_.toByte).toArray

   def convUtfToJefFull(data: String) : Array[Byte] = {
   data.flatMap( x =>Try(utfToJefFullData(x)).getOrElse("A2A3")).grouped(2).map( x =>Integer.parseInt(x.toString, 16)).map(_.toByte).toArray
   }
}