package d2k.common.df.template

import d2k.common.df.Executor
import org.apache.spark.sql.DataFrame
import d2k.common.df.template.base._
import d2k.common.InputArgs

trait FileToDb extends FileToAny[DataFrame] with AnyToDb[Unit] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}

trait FileToDf extends FileToAny[DataFrame] with AnyToDf[Unit] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}

trait FileToFile extends FileToAny[DataFrame] with AnyToFile[Unit] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}

trait FileToPq_Db extends FileToAny[DataFrame] with AnyToPq_Db[Unit] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}

trait FileToPq extends FileToAny[DataFrame] with AnyToPq[Unit] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}

trait FileToVal[T] extends FileToAny[T] with AnyToVal[Unit, T] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
