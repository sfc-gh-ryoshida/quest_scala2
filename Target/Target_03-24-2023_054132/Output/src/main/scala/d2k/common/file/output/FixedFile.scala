package d2k.common.file.output

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.SaveMode
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.Row
import org.apache.hadoop.io.NullWritable
import java.nio.file.Files
import java.nio.file.Path
import java.io.File
import java.io.FileSystem
import java.nio.file.FileSystems
import scala.reflect.io.Directory
import d2k.common.InputArgs
import d2k.common.fileConv.DomainProcessor
import d2k.common.Logging
import spark.common.FileCtl
import spark.common.SparkContexts
import spark.common.DfCtl
import DfCtl._
import DfCtl.implicits._
import com.snowflake.snowpark.types._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.LinkedHashSet
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class FixedFile (fileName: String, writeFilePartitionColumns: Seq[String] = Seq.empty[String], writeFilePartitionExtention: String = "") extends Logging {
   val charSet = "MS932"

   val pad = " ".getBytes(charSet).head

   def writeSingle_MS932(itemLengths: Seq[Int]) = (df: DataFrame) =>{
   if (writeFilePartitionColumns.isEmpty)
         {
         Directory(fileName).createDirectory(true, false)
Files.deleteIfExists(FileSystems.getDefault.getPath(fileName))
FileCtl.writeToFile(fileName){ pw => val collected = df.collect
elapse(s"fileWrite:${ fileName }"){
collected.foreach{ row =>pw.println(mkOutputStr(itemLengths)(row))}
}
}
         }
else
         {
         FileCtl.deleteDirectory(fileName)
elapse(s"fileWrite:${ fileName }"){
FileCtl.loanPrintWriterCache{ cache =>df.collect.foldLeft(cache){(l, r) =>FileCtl.writeToFileWithPartitionColumns(
fileName, partitionColumns = writeFilePartitionColumns, partitionExtention = writeFilePartitionExtention)(
mkOutputStr(itemLengths))(l)(r)
}
}
}
         }
   }

   def writeHdfs_MS932(itemLengths: Seq[Int]) = (df: DataFrame) =>{
   val partCheckeddDf = if (writeFilePartitionColumns.isEmpty)
         {
         val paddedDf = df.na.fill("") ~> paddingSpace(itemLengths)
 val sch = StructType(Seq(StructField("str", StringType, true)))
 val rows = paddedDf.rdd.map( r =>Row(r.toSeq.mkString("")))
SparkContexts.context.createDataFrame(rows, sch).write
         }
else
         {
         val targetSchemas = writeFilePartitionColumns.map{ n => val sc = df.schema(n)
sc.copy(dataType = StringType)
}
 val sch = StructType(targetSchemas ++ Seq(StructField("value", StringType, true)))
 val fieldNames = LinkedHashSet(df.schema.map(_.name) :_*)
 val rows = df.rdd.map{ row => val keyValues = writeFilePartitionColumns.map( n =>row.get(row.fieldIndex(n)).toString)
 val fixedValues = (fieldNames -- writeFilePartitionColumns).zip(itemLengths).foldLeft(ListBuffer.empty[String]){(l, r) =>l.append(paddingMS932(row.get(row.fieldIndex(r._1)), r._2))
l
}.mkString("")
Row((keyValues :+ fixedValues) :_*)
}
SparkContexts.context.createDataFrame(rows, sch).write.partitionBy(writeFilePartitionColumns :_*)
         }
partCheckeddDf.mode(SaveMode.Overwrite).text(fileName)
   }

   val paddingMS932 = (inTarget: Any, paddingSize: Int) =>{
   val strTarget = Option(inTarget).map(_.toString).getOrElse("")
 val targetBin = strTarget.getBytes("MS932")
 val itemSize = targetBin.length
if (paddingSize - itemSize > 0)
         {
         strTarget + (" " * (paddingSize - itemSize))
         }
else
         {
         new String (targetBin.take(paddingSize), "MS932")
         }
   }

   val paddingMS932Udf = udf{paddingMS932}

   def paddingSpace(targetLengths: Seq[Int]) = (df: DataFrame) =>{
   val items = df.schema.map(_.name).zip(targetLengths)
 val padSpace = items.map{
      case (n, l) => (n, paddingMS932Udf(df(n), lit(l)))
      }
df ~> editColumnsAndSelect(padSpace.e)
   }

   def writePartition_MS932(itemLengths: Seq[Int]) = (df: DataFrame) =>{
   import DfCtl.implicits._
if (writeFilePartitionColumns.isEmpty)
         {
         df.partitionWriteFile(
fileName, true, partitionExtention = writeFilePartitionExtention)(mkOutputStr(itemLengths))
         }
else
         {
         FileCtl.deleteDirectory(fileName)
df.partitionWriteToFileWithPartitionColumns(
fileName, writeFilePartitionColumns, true, partitionExtention = writeFilePartitionExtention)(mkOutputStr(itemLengths))
         }
   }

   def writeSequence_MS932(itemLengths: Seq[Int]) = (df: DataFrame) =>{
   FileCtl.deleteDirectory(fileName)
df.rdd.map( row =>(NullWritable.get, mkOutputBinary(itemLengths)(row))).saveAsSequenceFile(fileName, Some(classOf[org.apache.hadoop.io.compress.SnappyCodec]))
   }

   private [this] def mkOutputStr(itemLengths: Seq[Int])(row: Row) = itemLengths.zipWithIndex.foldLeft(new StringBuffer)((l, r) 
 =>l.append(new String (mkArrByte(row, r), charSet))).toString

   private [this] def mkOutputBinary(itemLengths: Seq[Int])(row: Row) = itemLengths.zipWithIndex.foldLeft(Array.empty[Byte])((l, r) =>l ++ mkArrByte(row, r))

   private [this] def mkArrByte(row: Row, itemInfo: (Int, Int)) = {
   val (len, idx) = itemInfo
Option(row.get(idx)).map{ x => val target = x.toString.getBytes(charSet)
if (len <= target.size)
         {
         target.take(len)
         }
else
         {
         target ++ Array.fill(len - target.size)(pad)
         }
}.getOrElse(Array.fill(len)(pad))
   }
}