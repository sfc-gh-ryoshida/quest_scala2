package d2k.common.df.executor

import com.snowflake.snowpark.DataFrame
import d2k.common.InputArgs
import com.snowflake.snowpark.functions._
import d2k.common.df.Executor
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait ConvNa extends Executor {
   val dateColumns: Seq[String]

   val tsColumns: Seq[String]

   def invoke(df: DataFrame)(implicit inArgs: InputArgs) : DataFrame = ConvNaTs(ConvNaDate(df, dateColumns), tsColumns)
}
trait ConvNaDate extends Executor {
   val dateColumns: Seq[String]

   def invoke(df: DataFrame)(implicit inArgs: InputArgs) : DataFrame = ConvNaDate(df, dateColumns)
}
 object ConvNaDate {
   val dateInit = "0001-01-01"

   def apply(df: DataFrame, dateColumnNames: Seq[String])(implicit inArgs: InputArgs) = df.na.fill(dateInit, dateColumnNames).na.replace(dateColumnNames, Map("" -> dateInit))
}
trait ConvNaTs extends Executor {
   val tsColumns: Seq[String]

   def invoke(df: DataFrame)(implicit inArgs: InputArgs) : DataFrame = ConvNaTs(df, tsColumns)
}
 object ConvNaTs {
   val tsInit = "0001-01-01 00:00:00"

   def apply(df: DataFrame, tsColumnNames: Seq[String])(implicit inArgs: InputArgs) = df.na.fill(tsInit, tsColumnNames).na.replace(tsColumnNames, Map("" -> tsInit))
}