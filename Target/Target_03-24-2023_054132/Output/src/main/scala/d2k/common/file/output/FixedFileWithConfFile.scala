package d2k.common.file.output

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._
import d2k.common.InputArgs
import scala.io.Source
import spark.common.FileCtl
import com.snowflake.snowpark._
import d2k.common.Logging
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object FixedFileWithConfFile {
   def confs(confPath: String) = {
   Source.fromFile(confPath).getLines.map{ line => val data = line.split("\t")
(data(0), data(1))
}.toMap
   }
}
 class FixedFileWithConfFile (fileName: String) (implicit inArgs: InputArgs) extends Serializable with Logging {
   val targetConf = FixedFileWithConfFile.confs(inArgs.fileConvOutputFile)

   val outputType = targetConf(s"${ inArgs.applicationId }.outputType")

   def writeFile(writeFileFunc: (DataFrame, InputArgs, Map[String, String]) => Unit) = (df: DataFrame) =>{
   if (writeFileFunc != null)
         {
         writeFileFunc(df, inArgs, targetConf)
         }
else
         {
         val output = outputType match {
               case "fixed" => writeFixedFile
               case _ => throw new RuntimeException (s"INVALID OUTPUT TYPE:${ outputType }")
            }
output(fileName, df)
         }
   }

   /*
   * 固定長ファイル出力
   */
   val writeFixedFile = (path: String, df: DataFrame) =>{
   def rpad(target: String, len: Int, pad: String = " ") = {
      val str = if (target == null)
            {
            ""
            }
else
            {
            target
            }
 val strSize = str.getBytes("MS932").size
 val padSize = len - strSize
s"${ str }${ pad * padSize }"
      }
 val itemLens = targetConf(s"${ inArgs.applicationId }.itemLengths")
 val itemLenList = itemLens.split(',').map{ len =>len.toInt}
 val itemLenListWithIdx = itemLenList.zipWithIndex
 def rowToFixedStr(row: Row) = {
      val line = itemLenListWithIdx.foldLeft(""){(acum, elem) =>{
         val (len, idx) = elem
 val str = row.getString(idx)
acum + rpad(str, len)
         }
}
line + "\n"
      }
FileCtl.writeToFile(path){ writer => val collected = df.rdd.map(rowToFixedStr).collect
elapse(s"fileWrite:${ path }"){
collected.foreach(writer.print)
}
}
   }
}