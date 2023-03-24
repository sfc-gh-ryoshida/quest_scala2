package d2k.common.df.template

import d2k.common.df.Executor
import com.snowflake.snowpark.DataFrame
import d2k.common.df.template.base._
import d2k.common.InputArgs
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait DfToDb extends DfToAny[DataFrame] with AnyToDb[DataFrame] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
trait DfToDf extends DfToAny[DataFrame] with AnyToDf[DataFrame] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
trait DfToFile extends DfToAny[DataFrame] with AnyToFile[DataFrame] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
trait DfToPq extends DfToAny[DataFrame] with AnyToPq[DataFrame] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
trait DfToVal[T] extends DfToAny[T] with AnyToVal[DataFrame, T] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}