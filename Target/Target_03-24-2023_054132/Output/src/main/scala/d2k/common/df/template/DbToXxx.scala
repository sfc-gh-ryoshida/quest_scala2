package d2k.common.df.template

import d2k.common.df.Executor
import com.snowflake.snowpark.DataFrame
import d2k.common.df.template.base._
import d2k.common.InputArgs
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait DbToDb extends DbToAny[DataFrame] with AnyToDb[Unit] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
trait DbToDf extends DbToAny[DataFrame] with AnyToDf[Unit] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
trait DbToFile extends DbToAny[DataFrame] with AnyToFile[Unit] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
trait DbToPq extends DbToAny[DataFrame] with AnyToPq[Unit] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
trait DbToVal[T] extends DbToAny[T] with AnyToVal[Unit, T] {
   self: Executor =>
   def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}