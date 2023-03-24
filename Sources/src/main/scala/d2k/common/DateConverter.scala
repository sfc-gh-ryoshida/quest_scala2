package d2k.common

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object DateConverter {
  object implicits {
    implicit class Dc(tm: Timestamp) {
      def toYmdhmsS =
        tm.toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"))
      def toYmdhms =
        tm.toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
      def toYmd =
        tm.toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    }
  }
}
