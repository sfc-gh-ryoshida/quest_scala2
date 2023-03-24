package d2k.common.df.template.base

import d2k.common.df.flow.OneInToOneOutForDf
import d2k.common.InputArgs
import d2k.common.df.Executor
import d2k.common.df.WritePq
import d2k.common.df.WriteDb
import com.snowflake.snowpark.DataFrame
import spark.common.PqCtl
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait AnyToPq_Db[IN] extends OneInToOneOutForDf[IN, DataFrame] with WritePq with WriteDb {
   override lazy val writePqName = writeTableName

   def postExec(df: DataFrame)(implicit inArgs: InputArgs) = {
   writeParquet(df)
 val pqCtl = new PqCtl (writePqPath)
writeDb(pqCtl.readParquet(writePqName))
   }
}