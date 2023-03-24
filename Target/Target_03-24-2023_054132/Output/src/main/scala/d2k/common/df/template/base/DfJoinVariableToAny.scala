package d2k.common.df.template.base

import scala.util.Try
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.Column
import spark.common.PqCtl
import d2k.common.InputArgs
import d2k.common.df.flow.OneInToOneOutForDf
import d2k.common.df.ReadFile
import d2k.common.fileConv.FileConv
import d2k.common.ResourceInfo
import d2k.common.df.InputInfo
import d2k.common.df.FileInputInfoBase
import d2k.common.df._
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait DfJoinVariableToAny[OUT] extends OneInToOneOutForDf[DataFrame, OUT] with ResourceInfo {
   val prefixName: String

   val joins: Seq[VariableJoin]

   def preExec(left: DataFrame)(implicit inArgs: InputArgs) : DataFrame = {
   val orgDf = left.columns.foldLeft(left)((df, name) =>df.withColumnRenamed(name, s"$prefixName#$name"))
joins.foldLeft(orgDf){(odf, vj) => val (joinDf, uniqId) = vj.inputInfo match {
         case x:PqInputInfoBase => (new PqCtl (x.inputDir(componentId)).readParquet(x.pqName), x.pqName)
         case x:FileInputInfoBase => {
         val fileDf = new FileConv (componentId, x, x.envName, true).makeDf
 val droppedRowErr = if (x.dropRowError)
               fileDf.drop("ROW_ERR").drop("ROW_ERR_MESSAGE")
else
               fileDf
(droppedRowErr, x.itemConfId)
         }
      }
 val joinedPrefixName = if (vj.prefixName.isEmpty)
         uniqId
else
         vj.prefixName
 val addNameDf = joinDf.columns.foldLeft(joinDf){(df, name) =>df.withColumnRenamed(name, s"${ joinedPrefixName }#${ name }")
}
 val joinedDf = odf.join(addNameDf, vj.joinExprs, "left_outer")
vj.dropCols.foldLeft(joinedDf)((l, r) =>l.drop(r))
}
   }
}