package spark.common

import java.io.PrintWriter
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.FileInputStream
import java.util.Properties
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.reflect.io.Directory
import com.snowflake.snowpark.Row
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object FileCtl {
   def writeToFile(fileName: String, append: Boolean = false, charEnc: String = "MS932")(func: PrintWriter => Unit) {
      val outFile = new PrintWriter (new OutputStreamWriter (new FileOutputStream (fileName, append), charEnc))
func(outFile)
outFile.close()
   }

   def emptyPrintWriterCache = Map.empty[String, PrintWriter]

   def loanPrintWriterCache(func: Map[String, PrintWriter] => Map[String, PrintWriter]) {
      var pwMap = FileCtl.emptyPrintWriterCache
try
         {pwMap = func(pwMap)}
      finally
         {
         pwMap.values.foreach(_.close)
         }
   }

   def addExtention(path: String, ext: String) = if (ext.isEmpty)
      path
else
      s"${ path }.${ ext }"

   def writeToFileWithPartitionColumns(fileName: String, partitionIndex: Int = 0, charEnc: String = "MS932", partitionColumns: Seq[String] = Seq.empty[String], partitionExtention: String = "")(func: Row => String)(pwCache: Map[String, PrintWriter])(row: Row) = {
   val outPath = partitionColumns.map{ col =>s"${ col }=${ row.getAs[String](col) }"
}.mkString(s"${ fileName }/", "/", "")
FileCtl.createDirectory(outPath)
 val outFile = pwCache.getOrElse(outPath, 
new PrintWriter (new OutputStreamWriter (new FileOutputStream (
addExtention(s"${ outPath }/${ partitionIndex }", partitionExtention), true), charEnc)))
outFile.println(func(row))
if (pwCache.isDefinedAt(outPath))
         pwCache
else
         pwCache.updated(outPath, outFile)
   }

   def loadEnv(filePath: String) : Properties = {
   val env = new Properties ()
env.load(new FileInputStream (filePath))
env
   }

   def exists(filePath: String) : Boolean = {
   val conf = SparkContexts.sc.hadoopConfiguration
 val fs = FileSystem.get(conf)
Option(fs.globStatus(new Path (filePath))).map(!_.isEmpty).getOrElse(false)
   }

   def createDirectory(fullPath: String) {
      Directory(fullPath).createDirectory(true, false)
   }

   def createDirectory(dirPath: String, filePath: String) {
      (Directory(dirPath) / filePath).createDirectory(true, false)
   }

   def deleteDirectory(fullPath: String) {
      Directory(fullPath).deleteRecursively
   }

   def deleteDirectory(dirPath: String, filePath: String) {
      (Directory(dirPath) / filePath).deleteRecursively
   }
}