package d2k.common

import org.joda.time.DateTime
import java.math.{ BigDecimal => jBigDecimal }
import org.apache.spark.sql.DataFrame

object SparkApp {
  object implicits {
    implicit class JbdToSbd(jbd: jBigDecimal) {
      def toBd = BigDecimal(jbd)
    }
  }
}

trait SparkApp {
  val DATE_FORMAT = "yy/MM/dd HH:mm:ss"

  def exec(implicit inArgs: InputArgs): DataFrame

  private [this] def runner(args: Array[String], isDebug: Boolean = false) {
    println(s"${new DateTime toString (DATE_FORMAT)} INFO START")
    val inputArgs = if (args.length == 8) {
      InputArgs(args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))
    } else {
      InputArgs(args(0), args(1), args(2), args(3))
    }
    if (isDebug) { println(inputArgs) }
    try exec(inputArgs.copy(isDebug = isDebug)).sparkSession.stop catch {
      case e: Throwable => println(s"${new DateTime toString (DATE_FORMAT)} ERROR ${e.toString()}"); throw e
    }
    println(s"${new DateTime toString (DATE_FORMAT)} INFO FINISHED")
  }

  def main(args: Array[String]) {
    runner(args)
  }

  def debug(args: Array[String]) {
    runner(args, true)
  }

}
