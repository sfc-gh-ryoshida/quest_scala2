package d2k.common

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class DateConverterTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "DateConverter" should {
   "success convert" when {
      val target = MakeDate.timestamp_yyyyMMddhhmmssSSS("00010101000000000")
"yyyyMMddhhmmssSSS" in {
         import DateConverter.implicits._
target.toYmdhmsS mustBe "00010101000000000"
         }
"yyyyMMddhhmmss" in {
         import DateConverter.implicits._
target.toYmdhms mustBe "00010101000000"
         }
"yyyyMMdd" in {
         import DateConverter.implicits._
target.toYmd mustBe "00010101"
         }
      }
   }
}