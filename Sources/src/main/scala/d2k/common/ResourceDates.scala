package d2k.common

import org.joda.time.format.DateTimeFormat
import spark.common.SparkContexts
import java.sql.Timestamp
import java.sql.Date

trait ResourceDates {
  val runningDates: Array[String]
  val runningSQLDate: Date

  val sysDateTime = SparkContexts.processTime
  val sysSQLDate = new Timestamp(sysDateTime.getTime)
  val runningDateYMD = runningDates(0)
  val runningYesterdayDateYMD = runningDates(1)
  val runningBeforeMonth = runningDates(3)
  val runningCurrentMonth = runningDates(6)

  val runningDateFmt = s"${runningDateYMD.take(4)}-${runningDateYMD.drop(4).take(2)}-${runningDateYMD.drop(6)}"

  val runningDate = RunningDate(runningDates)
  case class RunningDate(runningDates: Array[String]) {
    /** 運用日 */
    val MANG_DT = runningDates(0)
    /** 前日 */
    val YST_DY = runningDates(1)
    /** 翌日 */
    val NXT_DT = runningDates(2)
    /** 前月 */
    val BEF_MO = runningDates(3)
    /** 前月_月初日 */
    val BEF_MO_FRST_MTH_DT = runningDates(4)
    /** 前月_月末日 */
    val BEF_MO_MTH_DT = runningDates(5)
    /** 当月 */
    val CURR_MO = runningDates(6)
    /** 当月_月初日 */
    val CURR_MO_FRST_MTH_DT = runningDates(7)
    /** 当月_月末日 */
    val CURR_MO_MTH_DT = runningDates(8)
    /** 翌月 */
    val NXT_MO = runningDates(9)
    /** 翌月_月初日 */
    val NXT_MO_FRST_MTH_DT = runningDates(10)
    /** 翌月_月末日 */
    val NXT_MO_MTH_DT = runningDates(11)
  }
}
