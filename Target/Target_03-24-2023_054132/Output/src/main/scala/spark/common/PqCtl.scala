package spark.common

import SparkContexts._
import com.snowflake.snowpark.DataFrame
import scala.util.Try
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import d2k.common.MakeResource
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class PqCtl (val baseParquetFilePath: String) extends Serializable {
val logger = LoggerFactory.getLogger(this.getClass)
 def readParquet(appendPath: String, strictMode: Boolean = true, readPqEmptySchema: Seq[(String, String)] = Seq.empty[(String, String)]) = {
   val appendPaths = appendPath.split(",").map( path =>s"${ baseParquetFilePath }/${ path.trim }")
if (strictMode)
         {
         context.read.parquet(appendPaths :_*)
         }
else
         {
         try
               {context.read.parquet(appendPaths :_*)}
            catch {
               case t:org.apache.spark.sql.AnalysisException => {
               if (t.getMessage.startsWith("Path does not exist"))
                     {
                     logger.warn(s"Not Found Read Parquet[${ appendPaths.mkString(",") }]")
 val schema = MakeResource.makeSchema(readPqEmptySchema.map(_._1), readPqEmptySchema.map(_._2), readPqEmptySchema.map(_ =>"10"))
context.createDataFrame(context.emptyDataFrame.rdd, schema)
                     }
else
                     {
                     throw t
                     }
               }
            }
         }
   }
 object implicits {
      implicit class MyParquetDF (df: DataFrame) {
         def writeParquet(appendPath: String) = {
         df.write.mode("overwrite").parquet(s"${ baseParquetFilePath }/${ appendPath }")
         }

         def writeParquetWithPartitionBy(appendPath: String, partitionColumn: String*) = {
         try
               {df.write.mode("overwrite").partitionBy(partitionColumn :_*).parquet(s"${ baseParquetFilePath }/${ appendPath }")}
            catch {
               case t:NullPointerException => if (df.count() > 0)
                  {
                  df.show();throw t
                  }
else
                  {
                  println(s"${ baseParquetFilePath }/${ appendPath } IS NO RECORD")
                  }
               case t:Throwable => throw t
            }
         }
      }
   }
import implicits._
 def readParquetAndWriteParquet(readParquetPath: String, writeParquetPath: String)(proc: DataFrame => DataFrame = df =>df) = proc(readParquet(readParquetPath)).writeParquet(writeParquetPath)
 def readParquetAndWriteParquetWithPartitionBy(readParquetPath: String, writeParquetPath: String, partitionColumn: String*)(proc: DataFrame => DataFrame = df =>df) = proc(readParquet(readParquetPath)).writeParquetWithPartitionBy(writeParquetPath, partitionColumn :_*)
 def dbToPq(tableName: String, where: Array[String]) {
      dbToPq(tableName, tableName, where, DbCtl.dbInfo1)
   }
 def dbToPq(appendPath: String, tableName: String, where: Array[String], dbInfo: DbInfo) {
      new DbCtl (dbInfo).readTable(tableName, where).writeParquet(appendPath)
   }
 def dbToPq(appendPath: String, tableName: String, dbInfo: DbInfo) {
      new DbCtl (dbInfo).readTable(tableName).writeParquet(appendPath)
   }
}