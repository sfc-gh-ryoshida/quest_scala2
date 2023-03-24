package d2k.common.df

import d2k.common.ResourceInfo
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._
import d2k.common.InputArgs
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object DbCommonColumnAppender {
   def apply(df: DataFrame, componentId: String)(implicit inArgs: InputArgs) = {
   val addCommonItems = df.withColumn("DT_D2KMKDTTM", lit(inArgs.sysSQLDate)).withColumn("ID_D2KMKUSR", lit(componentId)).withColumn("DT_D2KUPDDTTM", lit(inArgs.sysSQLDate)).withColumn("ID_D2KUPDUSR", lit(componentId)).withColumn("NM_D2KUPDTMS", lit("0")).withColumn("FG_D2KDELFLG", lit("0"))
 val comonColumnNames = Array("DT_D2KMKDTTM", "ID_D2KMKUSR", "DT_D2KUPDDTTM", "ID_D2KUPDUSR", "NM_D2KUPDTMS", "FG_D2KDELFLG")
 val otherColumns = addCommonItems.columns
 val dropCommonColumns = comonColumnNames.foldLeft(otherColumns){(l, r) =>l.filter(_ != r)}
 val moveToFrontColumns = comonColumnNames ++ dropCommonColumns
addCommonItems.select(moveToFrontColumns.head, moveToFrontColumns.drop(1).toSeq :_*)
   }
}