package d2k.common

import java.sql.Timestamp
import java.time.LocalDateTime
import java.sql.Date

object MakeDate {
  def timestamp_yyyyMMdd(str: String) =
    Timestamp.valueOf(LocalDateTime.of(str.take(4).toInt, str.drop(4).take(2).toInt, str.drop(6).take(2).toInt, 0, 0, 0))

  def timestamp_yyyyMMddhhmmss(str: String) =
    Timestamp.valueOf(LocalDateTime.of(
      str.take(4).toInt, str.drop(4).take(2).toInt, str.drop(6).take(2).toInt,
      str.drop(8).take(2).toInt, str.drop(10).take(2).toInt, str.drop(12).take(2).toInt))

  def timestamp_yyyyMMddhhmmssSSS(str: String) =
    Timestamp.valueOf(LocalDateTime.of(
      str.take(4).toInt, str.drop(4).take(2).toInt, str.drop(6).take(2).toInt,
      str.drop(8).take(2).toInt, str.drop(10).take(2).toInt, str.drop(12).take(2).toInt, str.drop(14).toInt * 1000000))

  def date_yyyyMMdd(str: String) = new Date(timestamp_yyyyMMdd(str).getTime)

  object implicits {
    implicit class MakeDate(str: String) {
      def toDt = new Date(str.toTm.getTime)
      def toTm = {
        str.size match {
          case 8 => timestamp_yyyymmdd(str.take(4).toInt, str.drop(4).take(2).toInt, str.drop(6).take(2).toInt)
          case 14 => Timestamp.valueOf(LocalDateTime.of(
            str.take(4).toInt, str.drop(4).take(2).toInt, str.drop(6).take(2).toInt,
            str.drop(8).take(2).toInt, str.drop(10).take(2).toInt, str.drop(12).take(2).toInt))
        }
      }
    }
  }

  def timestamp_yyyymmdd(yyyy: Int, mm: Int, dd: Int) =
    Timestamp.valueOf(LocalDateTime.of(yyyy, mm, dd, 0, 0, 0))

  val date_00010101 = LocalDateTime.of(1, 1, 1, 0, 0, 0)
  val date_99991231 = LocalDateTime.of(9999, 12, 31, 0, 0, 0)
  val timestamp_00010101 = Timestamp.valueOf(LocalDateTime.of(1, 1, 1, 0, 0, 0))
  val timestamp_00010102 = Timestamp.valueOf(LocalDateTime.of(1, 1, 2, 0, 0, 0))
  val timestamp_00010103 = Timestamp.valueOf(LocalDateTime.of(1, 1, 3, 0, 0, 0))
  val timestamp_00010104 = Timestamp.valueOf(LocalDateTime.of(1, 1, 4, 0, 0, 0))
  val timestamp_00010105 = Timestamp.valueOf(LocalDateTime.of(1, 1, 5, 0, 0, 0))
  val timestamp_00010106 = Timestamp.valueOf(LocalDateTime.of(1, 1, 6, 0, 0, 0))
  val timestamp_00010107 = Timestamp.valueOf(LocalDateTime.of(1, 1, 7, 0, 0, 0))
  val timestamp_99991231 = Timestamp.valueOf(date_99991231)
}
