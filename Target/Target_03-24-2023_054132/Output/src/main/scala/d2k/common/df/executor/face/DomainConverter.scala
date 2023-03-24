package d2k.common.df.executor.face

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._
import d2k.common.InputArgs
import d2k.common.df.Executor
import d2k.common.fileConv.DomainProcessor._
import d2k.common.Udfs._
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait DomainConverter extends Executor {
   /**
   *  Set[(カラム名,ドメイン名)]
   */
   val targetColumns: Set[(String, String)]

   def invoke(orgDf: DataFrame)(implicit inArgs: InputArgs) : DataFrame = targetColumns.foldLeft(orgDf.na.fill("", targetColumns.map(_._1).toSeq)){(df, t) => val (name, domain) = t
 val convedColumn = domain match {
      case "年月日" => MakeDate.date_yyyyMMdd(domainConvert(col(name), lit(domain))).cast("date")
      case "年月日時分秒" => MakeDate.timestamp_yyyyMMddhhmmss(domainConvert(col(name), lit(domain))).cast("timestamp")
      case "年月日時分ミリ秒" => MakeDate.timestamp_yyyyMMddhhmmssSSS(domainConvert(col(name), lit(domain))).cast("timestamp")
      case _ => domainConvert(col(name), lit(domain))
   }
df.withColumn(name, convedColumn)
}
}