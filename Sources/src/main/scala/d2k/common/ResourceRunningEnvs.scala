package d2k.common

import org.joda.time.format.DateTimeFormat
import spark.common.SparkContexts
import java.sql.Timestamp
import java.sql.Date
import scala.util.Try
import scala.reflect.io.Path
import scala.io.Source

trait ResourceEnvs {
  val runningEnv = RunningEnv()
  case class RunningEnv() {
    val envData = Try {
      val fileDir = Path(sys.env("OM_RUNNING_ENV_DIR"))
      val fullPath = fileDir / s"${sys.env("OM_ROOT_NET_ID")}_runningEnv.properties"
      val lines = Source.fromFile(fullPath.toString).getLines
      lines.map { l =>
        val splitted = l.split('=')
        (splitted(0), splitted(1))
      }.toMap
    }.getOrElse(Map.empty[String, String])

    /** 日次実行数 */
    val DAILY_EXECUTE_CNT = envData.get("DAILY_EXECUTE_CNT").getOrElse("")
  }
}
