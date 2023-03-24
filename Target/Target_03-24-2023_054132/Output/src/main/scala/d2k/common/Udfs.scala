package d2k.common

import com.snowflake.snowpark.Column
import com.snowflake.snowpark.functions._
import java.sql.Date
import java.time.temporal.ChronoUnit
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.reflect.runtime.universe
import java.util.Locale
import java.text.SimpleDateFormat
import org.joda.time.DateTime
import org.joda.time.Duration
import org.joda.time.format.DateTimeFormat
import scala.util.Try
import d2k.common.fileConv.DomainProcessor._
import d2k.common.{PostCodeNormalizer => pcn}
import java.time.chrono._
import java.time.format._
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object Udfs {
   val nullToEmpty = udf{(str: String) =>if (str == null)
      {
      ""
      }
else
      {
      str
      }}

   val strcat = udf{(str1: String, str2: String) =>str1 + str2}

   val udf_takek = udf{(str: String, len: Int) => val byteStr = str.getBytes("MS932")
new String (byteStr.toList.take(len).toArray, "MS932")
}

   def takek(inStr: Column, len: Int) = udf_takek(inStr, lit(len))

   val selectNonNull = udf{(t1: String, t2: String) =>(t1, t2) match {
      case (null, null) => ""
      case (t1, null) => t1
      case (null, t2) => t2
      case (t1, t2) => t2
   }
}

   val rtrimk = udf{(_ : String).reverse.dropWhile(_ == '　').reverse}

   val udf_rpadk = udf{(inStr: String, len: Int, pad: String) => val str = if (inStr == null)
      {
      ""
      }
else
      {
      inStr
      }
 val strSize = str.getBytes("MS932").size
 val padSize = len - strSize
s"${ str }${ pad * padSize }"
}

   def rpadk(str: Column, len: Int, pad: String) = udf_rpadk(str, lit(len), lit(pad))

   val validPattern = (0 to 4).map(_.toString)

   val udf_dateCnvJpToEu = udf{(orgEra: String, jpDate: String, default: String) =>{
   //JDK11ではREIWAだけprivateなのでvaluesで代替
   val jpEras = JapaneseEra.values()
 val eraNum = orgEra match {
         case "R" => 4
         case "H" => 3
         case "S" => 2
         case "T" => 1
         case "M" => 0
         case _ => if (validPattern.contains(orgEra))
            orgEra.toInt
else
            -1
      }
 val res = if (eraNum >= 0 && !jpDate.isEmpty)
         {
         Try{
            val yy = jpDate.take(2).toInt
 val mm = jpDate.slice(2, 4).toInt
 val dd = jpDate.takeRight(2).toInt

               //グレゴリオ暦の導入が明治6年なのでJapaneseDateはそれ以降しか対応していないので別途計算
               if (eraNum == 0 && yy < 6)
                  {
                  "%d%02d%02d".format(1867 + yy, mm, dd)
                  }
else
                  {
                  val date = JapaneseDate.of(jpEras(eraNum), yy, mm, dd); val dateStr = date.format(DateTimeFormatter.BASIC_ISO_DATE)
dateStr
                  }
            }.getOrElse(default)
         }
else
         {
         default
         }
res
   }
}

   def dateCnvJpToEu(era: Column, jpDate: Column) = udf_dateCnvJpToEu(era, jpDate, lit("00010101"))

   def dateCnvJpToEuWithDefault(era: Column, jpDate: Column, default: Column) = udf_dateCnvJpToEu(era, jpDate, default)

   val kanaConv = udf{KanaConverter(_ : String)}

   val kanaConvSearch = udf{KanaConverter.select(_ : String)}

   def dateCalc(inDt: DateTime, month: Int, day: Int) = {
   val calcDay = (dt: DateTime) =>if (day != 0)
         dt.plusDays(day)
else
         dt
 val calcMonth = (dt: DateTime) =>if (month != 0)
         dt.plusMonths(month)
else
         dt
Option(inDt).map((calcMonth andThen calcDay)(_)).getOrElse(inDt)
   }

   def dateDurationByMonths(from: DateTime, to: DateTime) = {
   val monthDuration = (dt: DateTime) =>(dt.getYear * 12) + dt.getMonthOfYear
monthDuration(to) - monthDuration(from)
   }

   def dateDurationByDays(from: DateTime, to: DateTime) = {
   (new Duration (from, to)).getStandardDays
   }

   val parseDatePattern = DateTimeFormat.forPattern("yyyyMMdd")

   val outputDatePattern = "yyyyMMdd"

   object date {
      object ym {
         object string {
            val calc = udf{(dt: String, months: Int) => val result = Udfs.dateCalc(DateTime.parse(s"${ dt }01", parseDatePattern), months, 0)
result.toString(outputDatePattern).take(6)
}

            val duration = udf{(inDt1: String, inDt2: String) => val dt1 = DateTime.parse(s"${ inDt1 }01", parseDatePattern)
 val dt2 = DateTime.parse(s"${ inDt2 }01", parseDatePattern)
Udfs.dateDurationByMonths(dt1, dt2)
}
         }
      }

      object ymd {
         object string {
            val calc = udf{(dt: String, days: Int) => val result = Udfs.dateCalc(DateTime.parse(s"${ dt }", parseDatePattern), 0, days)
result.toString(outputDatePattern)
}

            val duration = udf{(inDt1: String, inDt2: String) => val dt1 = DateTime.parse(inDt1, parseDatePattern)
 val dt2 = DateTime.parse(inDt2, parseDatePattern)
Udfs.dateDurationByDays(dt1, dt2)
}

            val calcAndDiff = udf{(inTarget: String, inBase: String, beginDaysDiff: Int, endDaysDiff: Int) => def exec = for {
               target <- Option(inTarget)
 base <- Option(inBase)
            } yield {
            val targetDate = DateTime.parse(target.take(8), parseDatePattern)
 val beginBaseDate = Udfs.dateCalc(DateTime.parse(base.take(8), parseDatePattern), 0, beginDaysDiff)
 val endBaseDate = Udfs.dateCalc(DateTime.parse(base.take(8), parseDatePattern), 0, endDaysDiff)
(targetDate.isAfter(beginBaseDate) || targetDate.isEqual(beginBaseDate)) && (targetDate.isBefore(endBaseDate) || targetDate.isEqual(endBaseDate))
            }
Try(exec.getOrElse(true)).getOrElse(true)
}

            def calcAndDiff(base: String, beginDaysDiff: Int, endDaysDiff: Int = 0)(targets: Column*) : Column = {
            targets.foldLeft(lit(false)){
               case (result, target) => {
               result || calcAndDiff(target, lit(base), lit(beginDaysDiff), lit(endDaysDiff))
               }
               }
            }
         }
      }
   }

   val hashAuId = udf{(target: String) =>target match {
      case null => null
      case "" => ""
      case targetif (target.forall(_.equals(' '))) => target
      case _ => MD5Utils.getMD5Base64Str(target, "THINK!KRI")
   }
}

   val addPointToDateString = udf{(target: String) =>target match {
      case null => null
      case "" => ""
      case _ => target.take(14) + "." + target.drop(14)
   }
}

   val addHyphenToDateString = udf{(target: String) =>target match {
      case null => null
      case "" => ""
      case _ => target.take(4) + "-" + target(4) + target(5) + "-" + target(6) + target(7)
   }
}

   val blankToZero = udf{(target: String) =>target match {
      case null => "0"
      case "" => "0"
      case _ => target
   }
}

   val trancateAfterPoint = udf{(target: String) =>target match {
      case null => null
      case "" => ""
      case _ => target.split('.')(0)
   }
}

   object DateRangeStatus {
      val STATUS_BEFORE = "0"

      val STATUS_ON = "1"

      val STATUS_END = "9"

      val getStatus = udf{(beginDate: String, endDate: String, runningDateYMD: String) =>getDateRangeStatus(removeHyphen(beginDate), removeHyphen(endDate), runningDateYMD)
}

      val getStatusWithDeleteDate = udf{(beginDate: String, endDate: String, deleteDate: String, runningDateYMD: String) =>deleteDate match {
         case targetif !isBlankDate(removeHyphen(target)) => STATUS_END
         case _ => getDateRangeStatus(removeHyphen(beginDate), removeHyphen(endDate), runningDateYMD)
      }
}

      val getStatusWithBlankReplace = udf{(beginDate: String, endDate: String, runningDateYMD: String) =>getDateRangeStatus(blankToMaxDate(removeHyphen(beginDate)), blankToMaxDate(removeHyphen(endDate)), runningDateYMD)
}

      val getStatusWithBlankReplaceAndDeleteDate = udf{(beginDate: String, endDate: String, deleteDate: String, runningDateYMD: String) =>deleteDate match {
         case targetif !isBlankDate(removeHyphen(target)) => STATUS_END
         case _ => getDateRangeStatus(removeHyphen(beginDate), blankToMaxDate(removeHyphen(endDate)), runningDateYMD)
      }
}

      def getDateRangeStatus(beginDate: String, endDate: String, runningDateYMD: String) = {
      runningDateYMD match {
            case targetif target < nullToMinDate(beginDate) => STATUS_BEFORE
            case targetif target >= nullToMaxDate(endDate) => STATUS_END
            case _ => STATUS_ON
         }
      }

      def isBlankDate(target: String) = {
      target match {
            case null => true
            case "" => true
            case "00010101" => true
            case _ => false
         }
      }

      def removeHyphen(target: String) = {
      target match {
            case null => null
            case "" => ""
            case _ => target.replaceAll("-", "")
         }
      }

      def blankToMaxDate(target: String) = if (isBlankDate(target))
         "99991231"
else
         target

      def nullToMinDate(target: String) = if (target == null)
         "00010101"
else
         target

      def nullToMaxDate(target: String) = if (target == null)
         "99991231"
else
         target
   }

   val isNaOrNull = udf{(target: String) =>target == null}

   val isNotNaAndNotNull = udf{(target: String) =>target != null}

   val cutLimitStr = udf{(target: String, threshold: Int) => val cut = () =>{
   val byteSize = (_ : String).getBytes("MS932").size
(1 to byteSize(target) - threshold).map( i =>(i, byteSize(target.dropRight(i)))).filter(_._2 <= threshold).headOption.map( d =>target.dropRight(d._1)).getOrElse(target)
   }
target match {
      case null => null
      case "" => ""
      case _ => cut()
   }
}

   val calcSchoolAge = udf{(targetBirthDate: java.sql.Timestamp, runningDateYMD: String) => val calc = (birthDateYMD: String) =>{
   val (runningDateY, runningDateMD) = runningDateYMD.splitAt(4)
 val (birthDateY, birthDateMD) = birthDateYMD.splitAt(4)
 val isRunDateIn0402_1231 = runningDateMD >= "0402" && runningDateMD <= "1231"
 val isBirthDateIn0402_1231 = birthDateMD >= "0402" && birthDateMD <= "1231"
 val schoolAge = (isRunDateIn0402_1231, isBirthDateIn0402_1231) match {
         case (true, true) => runningDateY.toInt - birthDateY.toInt
         case (true, false) => (runningDateY.toInt + 1) - birthDateY.toInt
         case (false, true) => runningDateY.toInt - birthDateY.toInt - 1
         case (false, false) => runningDateY.toInt - birthDateY.toInt
      }
if (0 <= schoolAge && schoolAge <= 999)
         {
         schoolAge
         }
else
         {
         999
         }
   }
 val schoolAge = targetBirthDate match {
      case null => 999
      case _ => calc(targetBirthDate.toString.take(10).replace("-", ""))
   }
new Integer (schoolAge)
}

   def strToDt(dateStr: String) = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"))

   val calcAge = udf{(targetBirthDate: java.sql.Timestamp, runningDateYMD: String) =>Try{
ChronoUnit.YEARS.between(targetBirthDate.toLocalDateTime.toLocalDate, strToDt(runningDateYMD)).toInt
}.map( x =>if (x < 0 || x > 999)
      999
else
      x).getOrElse(999)
}

   val domainConvert = udf{(target: String, domain: String) =>exec(domain, target).right.get}

   object MakeDate {
      val date_yyyyMMdd = udf{(target: String) =>d2k.common.MakeDate.date_yyyyMMdd(target)}

      val timestamp_yyyyMMdd = udf{(target: String) =>d2k.common.MakeDate.timestamp_yyyyMMdd(target)}

      val timestamp_yyyyMMddhhmmss = udf{(target: String) =>d2k.common.MakeDate.timestamp_yyyyMMddhhmmss(target)}

      val timestamp_yyyyMMddhhmmssSSS = udf{(target: String) =>d2k.common.MakeDate.timestamp_yyyyMMddhhmmssSSS(target)}
   }

   object PostCodeNormalizer {
      val single = udf{(postCode: String) =>pcn.single(postCode)}

      val parent = udf{(postCode: String) =>pcn.parent(postCode)}

      val child = udf{(postCode: String) =>pcn.child(postCode)}
   }
}