package d2k.common.fileConv

import org.apache.spark.sql.functions._
import scala.util.Try
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import scala.BigInt
import scala.Left
import scala.Right
import scala.annotation.tailrec
import d2k.common.JefConverter

object DomainProcessor extends Serializable {
  val ERR_MSG_INVALID_VALUE = "不正な形式"
  val ERR_MSG_NOT_EXIST_VALUE = "存在しない値"
  val ERR_MSG_INVALID_DOMAIN = "不正な変換型"
  val ERR_MSG_INVALID_DATA_DIV = "不正なデータ区分"

  val FORMAT_YYYYMMDDHHMMSSMS = "yyyyMMddHHmmssSSS"
  val FORMAT_YYYYMMDD = "yyyyMMdd"
  val FORMAT_YYYYMM = "yyyyMM"
  val FORMAT_MMDD = "MMdd"
  val FORMAT_YYYYMMDDHHMMSS = "yyyyMMddHHmmss"
  val FORMAT_YYYYMMDDHH = "yyyyMMddHH"
  val FORMAT_HHMMSS = "HHmmss"
  val FORMAT_HHMMSSMS = "HHmmssSSS"
  val FORMAT_HHMM = "HHmm"
  val FORMAT_MMSS = "mmss"
  val FORMAT_YYYY = "yyyy"
  val FORMAT_MM = "MM"
  val FORMAT_DD = "dd"
  val FORMAT_HH = "HH"
  val FORMAT_MI = "mm"
  val FORMAT_SS = "ss"

  val FORMATTER_YYYYMMDDHHMMSSMS = DateTimeFormat.forPattern(FORMAT_YYYYMMDDHHMMSSMS)
  val FORMATTER_YYYYMMDD = DateTimeFormat.forPattern(FORMAT_YYYYMMDD)
  val FORMATTER_YYYYMM = DateTimeFormat.forPattern(FORMAT_YYYYMM)
  val FORMATTER_MMDD = DateTimeFormat.forPattern(FORMAT_MMDD)
  val FORMATTER_YYYYMMDDHHMMSS = DateTimeFormat.forPattern(FORMAT_YYYYMMDDHHMMSS)
  val FORMATTER_YYYYMMDDHH = DateTimeFormat.forPattern(FORMAT_YYYYMMDDHH)
  val FORMATTER_YYYYMMDDHHMM = DateTimeFormat.forPattern("yyyyMMddHHmm")
  val FORMATTER_HHMMSS = DateTimeFormat.forPattern(FORMAT_HHMMSS)
  val FORMATTER_HHMMSSMS = DateTimeFormat.forPattern(FORMAT_HHMMSSMS)
  val FORMATTER_HHMM = DateTimeFormat.forPattern(FORMAT_HHMM)
  val FORMATTER_MMSS = DateTimeFormat.forPattern(FORMAT_MMSS)

  val PD_SUFFIX = "_PD"
  val ZD_SUFFIX = "_ZD"

  val REC_DIV_NUMBER_HEAD = "1"
  val REC_DIV_NUMBER_DATA = "2"
  val REC_DIV_NUMBER_FOOT = "3"

  val REC_DIV_ALPHABET_HEAD = "H"
  val REC_DIV_ALPHABET_DATA = "D"
  val REC_DIV_ALPHABET_FOOT = "T"

  def exec(domain: String, data: String): Either[String, String] = dp {
    domain match {
      case "年月日"               => convDate(data, "00010101", "99991231", FORMATTER_YYYYMMDD)
      case "年月日_SL"            => convDate(data.replaceAll("/", ""), "00010101", "99991231", FORMATTER_YYYYMMDD)
      case "年月日_HY"            => convDate(data.replaceAll("-", ""), "00010101", "99991231", FORMATTER_YYYYMMDD)
      case "年月"                => convDate(data, "000101", "999912", FORMATTER_YYYYMM)
      case "年月_SL"             => convDate(data.replaceAll("/", ""), "000101", "999912", FORMATTER_YYYYMM)
      case "月日"                => convDate(data, "0101", "1231", FORMATTER_MMDD)
      case "年"                 => convDateParts(data, "0001", "9999")
      case "月"                 => convDateParts(data, "01", "12")
      case "日"                 => convDateParts(data, "01", "31")
      case "年月日時分秒"            => convDate(data, "00010101000000", "99991231235959", FORMATTER_YYYYMMDDHHMMSS)
      case "年月日時分秒_HC"         => convDate(data.replaceAll("[- :]", ""), "00010101000000", "99991231235959", FORMATTER_YYYYMMDDHHMMSS)
      case "年月日時分秒_SC"         => convDate(data.replaceAll("[/ :]", ""), "00010101000000", "99991231235959", FORMATTER_YYYYMMDDHHMMSS)
      case "年月日時分秒_CO"         => convDate(data.replaceAll("[ :]", ""), "00010101000000", "99991231235959", FORMATTER_YYYYMMDDHHMMSS)
      case "年月日時分ミリ秒"          => convTimeStamp(data, "00010101000000000", "99991231235959999", FORMATTER_YYYYMMDDHHMMSSMS, FORMAT_YYYYMMDDHHMMSSMS)
      case "年月日時分ミリ秒/ミリ秒小数点付加" => addPeriod(convTimeStamp(data, "00010101000000000", "99991231235959999", FORMATTER_YYYYMMDDHHMMSSMS, FORMAT_YYYYMMDDHHMMSSMS), 14)
      case "年月日時"              => convDate(data, "0001010100", "9999123123", FORMATTER_YYYYMMDDHH)
      case "年月日時分"             => convDate(data, "000101010000", "999912312359", FORMATTER_YYYYMMDDHHMM)
      case "年月日時分_SC"          => convDate(data.replaceAll("[/ :]", ""), "000101010000", "999912312359", FORMATTER_YYYYMMDDHHMM)
      case "時分秒"               => convDate(data, "000000", "235959", FORMATTER_HHMMSS)
      case "時分秒_CO"            => convDate(data.replaceAll(":", ""), "000000", "235959", FORMATTER_HHMMSS)
      case "時分ミリ秒"             => convTimeStamp(data, "000000000", "235959999", FORMATTER_HHMMSSMS, FORMAT_HHMMSSMS)
      case "時分ミリ秒/ミリ秒小数点付加"    => addPeriod(convTimeStamp(data, "000000000", "235959999", FORMATTER_HHMMSSMS, FORMAT_HHMMSSMS), 6)
      case "時分"                => convDate(data, "0000", "2359", FORMATTER_HHMM)
      case "時"                 => convDateParts(data, "00", "23")
      case "分"                 => convDateParts(data, "00", "59")
      case "秒"                 => convDateParts(data, "00", "59")
      case "時間"                => convTime(data, "000000", "995959")
      case "数字"                => convDigit(data)
      case "数字_SIGNED"         => convSignedDigit(data)
      case "Byte配列"            => Right(data)
      case "文字列"               => Right(data.trim)
      case "文字列_trim_無し"       => Right(data)
      case "文字列_trim_半角"       => Right(data.trim)
      case "文字列_trim_全角"       => Right(trimFull(data))
      case "文字列_trim_全半角"      => Right(trimFullAndHalf(data))
      case "全角文字列"             => Right(trimFull(data))
      case "全角文字列_trim_無し"     => Right(data)
      case "全角文字列_trim_全角"     => Right(trimFull(data))
      case "レコード区分_NUMBER"     => convDataDiv(data)
      case "レコード区分_ALPHABET"   => Right(data)
      case "通信方式"              => convCommMthd(data, "       ")
      case "識別子"               => Right(data.trim)
      case _                   => throw new RuntimeException(s"${ERR_MSG_INVALID_DOMAIN}:${domain}")
    }
  }

  def trimFull(data: String) = data.dropWhile(_ == '　').reverse.dropWhile(_ == '　').reverse
  def trimFullAndHalf(data: String) = data.dropWhile(s => s == '　' || s == ' ').reverse.dropWhile(s => s == '　' || s == ' ').reverse
  def execArrayByte(domain: String, data: Array[Byte], charEnc: String): Either[String, String] =
    data match {
      case target if domain == "Byte配列" => Right(new String(target, "ISO-8859-1"))

      case target if (isPd(domain) || isZd(domain)) && (isNull(target)) => exec(domain.dropRight(3), new String(data, charEnc))
      case target if (isPd(domain) || isZd(domain)) && (isEmpty(target)) => digitErrOrConvDate(domain.dropRight(3), new String(data, charEnc))

      case target if isPd(domain) && !isValidSign(target) => digitErrOrConvDate(domain.dropRight(3), new String(data, charEnc))
      case target if isPd(domain) => convDigitOrDatePd(domain.dropRight(3), target)

      case target if isZd(domain) && (!isValidSignZd(target)) => digitErrOrConvDate(domain.dropRight(3), new String(data, charEnc))
      case target if isZd(domain) => convZd(domain.dropRight(3), target)

      case target if JefConverter.isJefHalf(domain, charEnc) => exec(domain, JefConverter.convJefToUtfHalf(data))
      case target if JefConverter.isJefFull(domain, charEnc) => exec(domain, JefConverter.convJefToUtfFull(data))

      case _ => exec(domain, new String(data, charEnc))
    }

  def dp(proc: => Either[String, String]) =
    Try(proc).recover {
      case t: RuntimeException => throw t
      case t: Exception        => Left(t.toString)
    }.get

  /*
   * 日付系フォーマットあり 
   */
  def convDate(data: String, min: String, max: String, format: DateTimeFormatter) = data match {
    case target if (isNull(target))             => Right(min)
    case target if (isEmpty(target))            => Right(min)
    case target if (!isDigit(target))           => Right(min)
    case allNineRegex(_*)                       => Right(max)
    case target if (target.toLong < min.toLong) => Right(min)
    case target if (target.toLong > max.toLong) => Right(min)
    case target if (!isDate(target, format))    => Right(min)
    case target                                 => Right(target)
  }
  /*
   * TIMESTAMPは桁数不足のケースがある 
   */
  def convTimeStamp(data: String, min: String, max: String, formatter: DateTimeFormatter, format: String) = {
    val targetMinPad = data.padTo(format.size, '0')
    val targetMaxPad = data.padTo(format.size, '9')
    data match {
      case target if (isNull(target))                    => Right(min)
      case target if (isEmpty(target))                   => Right(min)
      case target if (!isDigit(target))                  => Right(min)
      case allNineRegex(_*)                              => Right(max)
      case target if (targetMinPad.toLong <= min.toLong) => Right(min)
      case target if (targetMaxPad.toLong > max.toLong)  => Right(min)
      case target if (!isDate(target, formatter))        => Right(min)
      case target                                        => Right(targetMinPad)
    }
  }

  /*
   * 日付系の一部分 
   */
  def convDateParts(data: String, min: String, max: String) = data match {
    case target if (isNull(target))           => Right(min)
    case target if (isEmpty(target))          => Right(min)
    case target if (!isDigit(target))         => Right(min)
    case allNineRegex(_*)                     => Right(max)
    case target if (target.toInt < min.toInt) => Right(min)
    case target if (target.toInt > max.toInt) => Right(min)
    case target                               => Right(target)
  }

  /*
   * 時間（時刻でない） 
   */
  def convTime(data: String, min: String, max: String) = data match {
    case target if (isNull(target)) => Right(min)
    case target if (isEmpty(target)) => Right(min)
    case target if (!isDigit(target)) => Right(min)
    case allNineRegex(_*) => Right(max)
    case target if (target.toInt < min.toInt) => Right(min)
    case target if (target.toInt > max.toInt) => Right(min)
    case target if (!isDate(target.drop(2), FORMATTER_MMSS)) => Right(min)
    case target => Right(target)
  }

  def padForDate(domain: String, data: String) = domain match {
    case "年月日"      => zeroPadLeft(data, FORMAT_YYYYMMDD.length)
    case "年月日_SL"   => throw new RuntimeException(s"${ERR_MSG_INVALID_DOMAIN}:${domain}")
    case "年月"       => zeroPadLeft(data, FORMAT_YYYYMM.length)
    case "月日"       => zeroPadLeft(data, FORMAT_MMDD.length)
    case "年"        => zeroPadLeft(data, FORMAT_YYYY.length)
    case "月"        => zeroPadLeft(data, FORMAT_MM.length)
    case "日"        => zeroPadLeft(data, FORMAT_DD.length)
    case "年月日時分秒"   => zeroPadLeft(data, FORMAT_YYYYMMDDHHMMSS.length)
    case "年月日時分ミリ秒" => zeroPadLeft(data, FORMAT_YYYYMMDDHHMMSSMS.length)
    case "年月日時"     => zeroPadLeft(data, FORMAT_YYYYMMDDHH.length)
    case "時分秒"      => zeroPadLeft(data, FORMAT_HHMMSS.length)
    case "時分秒_CO"   => throw new RuntimeException(s"${ERR_MSG_INVALID_DOMAIN}:${domain}")
    case "時分ミリ秒"    => zeroPadLeft(data, FORMAT_HHMMSSMS.length)
    case "時分"       => zeroPadLeft(data, FORMAT_HHMM.length)
    case "時"        => zeroPadLeft(data, FORMAT_HH.length)
    case "分"        => zeroPadLeft(data, FORMAT_MI.length)
    case "秒"        => zeroPadLeft(data, FORMAT_SS.length)
    case "時間"       => zeroPadLeft(data, FORMAT_HHMMSS.length)
    case _          => data
  }

  def convPd(domain: String, data: Array[Byte])(unpack: (Array[Byte] => String)) = {
    try {
      exec(domain, unpack(data))
    } catch {
      case t: NumberFormatException => Left(ERR_MSG_INVALID_VALUE)
      case t: Exception             => throw t
    }
  }

  def convDatePd(domain: String, data: Array[Byte]) = {
    try {
      val unpacked = unpackForNum(data)
      exec(domain, padForDate(domain, unpacked))
    } catch {
      case t: NumberFormatException => exec(domain, "")
      case t: Exception             => throw t
    }
  }

  def convDigitOrDatePd(domain: String, data: Array[Byte]) = {
    domain match {
      case "文字列" => convPd(domain, data)(unpackForStr)
      case "識別子" => convPd(domain, data)(unpackForId)
      case "数字"  => convPd(domain, data)(unpackForNum)
      case _     => convDatePd(domain, data)
    }
  }

  def digitErrOrConvDate(orgDomain: String, data: String): Either[String, String] =
    orgDomain match {
      case "数字" => Left(ERR_MSG_INVALID_VALUE)
      case _    => exec(orgDomain, data)
    }

  def convDataDiv(data: String) = data match {
    case REC_DIV_NUMBER_HEAD => Right(REC_DIV_ALPHABET_HEAD)
    case REC_DIV_NUMBER_DATA => Right(REC_DIV_ALPHABET_DATA)
    case REC_DIV_NUMBER_FOOT => Right(REC_DIV_ALPHABET_FOOT)
    case _                   => throw new RuntimeException(s"${ERR_MSG_INVALID_DATA_DIV}:${data}")
  }

  def convCommMthd(data: String, allSpace: String) = data match {
    case target if (isNull(target))  => Right("")
    case target if (isEmpty(target)) => Right("")
    case _                           => Right(data.trim().substring(0, 1))
  }

  def convSignedDigit(data: String) = {
    val default = "0"
    val num = if (data.isEmpty) { default } else { data }
    convDigit(Try { BigDecimal(num).toString }.getOrElse(default))
  }

  def unpackForNum(data: Array[Byte]) = {
    val str = data.foldLeft("") { (l, r) => l + f"$r%02x" }
    val decimal = BigInt(str.dropRight(1))
    val isMinus = str.takeRight(1) == "d"
    (if (isMinus) { -decimal } else { decimal }).toString
  }

  def unpackForStr(data: Array[Byte]) = {
    val str = data.foldLeft("") { (l, r) => l + f"$r%02x" }
    val decimal = str.dropRight(1)
    val isMinus = str.takeRight(1) == "d"
    (if (isMinus) { s"-$decimal" } else { decimal })
  }

  def unpackForId(data: Array[Byte]) =
    data.foldLeft("") { (l, r) => l + f"$r%02x" }.dropRight(1)

  def zeroPadLeft(target: String, fullLen: Int) = s"${"0" * (fullLen - target.length())}${target}"

  def isDate(target: String, format: DateTimeFormatter) =
    Try(format.withZoneUTC.parseDateTime(target)).map(_ => true).getOrElse(false)

  def isEmpty(target: String) = target.trim.isEmpty

  def isEmpty(target: Array[Byte]) = {
    target.foldLeft(true) { (l, r) => l && r == 0x20 }
  }

  def isNull(target: Array[Byte]) = {
    target.foldLeft(true) { (l, r) => l && r == 0x00 }
  }

  def isNull(target: String): Boolean = {
    isNull(target.getBytes("MS932"))
  }

  def isDigit(target: String) = Try(target.toLong).map(_ => true).getOrElse(false)

  def isPd(domain: String) = domain.endsWith(PD_SUFFIX)
  def isZd(domain: String) = domain.endsWith(ZD_SUFFIX)

  def convDigit(data: String) = data match {
    case target if (isNull(target.getBytes("MS932"))) => Right(target)
    case target if (!isDigit(target.trim()))          => Left(ERR_MSG_INVALID_VALUE)
    case target if (isEmpty(target))                  => Left(ERR_MSG_INVALID_VALUE)
    case target                                       => Right(target)
  }

  def isValidSign(target: Array[Byte]) = {
    val sign = target.last & 0x0F
    sign == 0x0d || sign == 0x0c || sign == 0x0f
  }

  def isValidSignZd(target: Array[Byte]) = {
    val sign = target.last & 0xF0
    sign == 0xf0 || sign == 0xc0 || sign == 0xd0
  }

  val allNineRegex = """^9*$""".r

  def addPeriod(target: Either[String, String], pos: Int) = {
    target.right.map(str => str.take(pos) + "." + str.drop(pos))
  }

  def convZd(domain: String, data: Array[Byte]) = {
    try {
      val unpacked = domain match {
        case "文字列" => unzoneForStr(data)
        case "識別子" => unzoneForId(data)
        case _     => unzone(data)
      }
      exec(domain, padForDate(domain, unpacked))
    } catch {
      case t: NumberFormatException => Left(ERR_MSG_INVALID_VALUE)
      case t: Exception             => throw t
    }
  }

  def unzone(data: Array[Byte]) = {
    val str = data.map(x => f"$x%02x".drop(1)).mkString
    val decimal = BigInt(str)
    val isMinus = data.map(x => f"$x%02x").mkString.reverse.apply(1) == 'd'
    (if (isMinus) { -decimal } else { decimal }).toString
  }

  def unzoneForStr(data: Array[Byte]) = {
    val decimal = data.map(x => f"$x%02x".drop(1)).mkString
    val isMinus = data.map(x => f"$x%02x").mkString.reverse.apply(1) == 'd'
    if (isMinus) { s"-$decimal" } else { decimal }
  }

  def unzoneForId(data: Array[Byte]) =
    data.map(x => f"$x%02x".drop(1)).mkString
}
