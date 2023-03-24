package d2k.common.df

import spark.common.DbInfo
import spark.common.PqCtl
import d2k.common.InputArgs
import d2k.common.ResourceInfo
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait WritePq extends ResourceInfo {
   lazy val writePqName: String = componentId

   def writePqPath(implicit inArgs: InputArgs) : String = inArgs.baseOutputFilePath

   val writePqPartitionColumns: Seq[String] = Seq.empty[String]

   def writeParquet(df: DataFrame)(implicit inArgs: InputArgs) = {
   val pqCtl = new PqCtl (writePqPath)
import pqCtl.implicits._
if (writePqPartitionColumns.isEmpty)
         {
         df.writeParquet(writePqName)
         }
else
         {
         df.writeParquetWithPartitionBy(writePqName, writePqPartitionColumns :_*)
         }
df.sqlContext.emptyDataFrame
   }
}