package d2k.common.df.template

import org.apache.spark.sql.DataFrame
import d2k.common.df.Executor
import d2k.common.df.template.base._
import d2k.common.InputArgs

trait PqToDb extends PqToAny[DataFrame] with AnyToDb[Unit] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}

trait PqToDf extends PqToAny[DataFrame] with AnyToDf[Unit] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}

trait PqToFile extends PqToAny[DataFrame] with AnyToFile[Unit] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}

trait PqToPq extends PqToAny[DataFrame] with AnyToPq[Unit] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}

trait PqToVal[T] extends PqToAny[T] with AnyToVal[Unit, T] { self: Executor =>
  def exec(df: DataFrame)(implicit inArgs: InputArgs) = self.invoke(df)
}
