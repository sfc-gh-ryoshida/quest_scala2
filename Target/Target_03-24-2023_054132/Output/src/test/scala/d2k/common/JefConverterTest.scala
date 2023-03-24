package d2k.common

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class JefConverterTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "JEF to UTF8" should {
   "be normal end." in {
      val data = Array[Short](0xA3, 0xCA, 0x40, 0x40, 0xA3, 0xC5, 0x40, 0x40, 0xA3, 0xC6).map(_.toByte)
JefConverter.convJefToUtfFull(data) mustBe "Ｊ　Ｅ　Ｆ"
      }
"illegal data pattern" in {
      val data = Array[Short](0xA3, 0xCA, 0xFF, 0xFF, 0xA3, 0xC5, 0x00, 0x00, 0xA3, 0xC6).map(_.toByte)
JefConverter.convJefToUtfFull(data) mustBe "Ｊ■Ｅ■Ｆ"
      }
"normal char code" in {
      val data = Array[Short](0xB9, 0xC2, 0xB3, 0xEB, 0xC6, 0xEA, 0x7F, 0xD9, 0x7F, 0xEA).map(_.toByte)
JefConverter.convJefToUtfFull(data) mustBe "溝葛楢∨♯"
      }
"kddi original char code" in {
      val data = Array[Short](0x57, 0xC6, 0x61, 0xB5, 0x70, 0xC5).map(_.toByte)
JefConverter.convJefToUtfFull(data) mustBe "溝葛楢"
      }
"kddi original char code 塚(旧文字)" in {
      val data = Array[Short](0xC4, 0xCD).map(_.toByte)
JefConverter.convJefToUtfFull(data) mustBe "塚"
      }
"0x00 half char to space" in {
      val data = Array[Short](0x00).map(_.toByte)
JefConverter.convJefToUtfHalf(data) mustBe " "
      }
"0x28 half char to space" in {
      val data = Array[Short](0x28).map(_.toByte)
JefConverter.convJefToUtfHalf(data) mustBe ""
      }
"0x29 half char to space" in {
      val data = Array[Short](0x29).map(_.toByte)
JefConverter.convJefToUtfHalf(data) mustBe ""
      }
"0x38 half char to space" in {
      val data = Array[Short](0x38).map(_.toByte)
JefConverter.convJefToUtfHalf(data) mustBe ""
      }
"0x05 half char to tab" in {
      val data = Array[Short](0x05).map(_.toByte)
JefConverter.convJefToUtfHalf(data) mustBe "\t"
      }
"0x4B half char to period" in {
      val data = Array[Short](0x4B).map(_.toByte)
JefConverter.convJefToUtfHalf(data) mustBe "."
      }
"0x79 half char to tab" in {
      val data = Array[Short](0x79).map(_.toByte)
JefConverter.convJefToUtfHalf(data) mustBe "`"
      }
"0x7D half char to single quote" in {
      val data = Array[Short](0x7D).map(_.toByte)
JefConverter.convJefToUtfHalf(data) mustBe "'"
      }
"0x7E half char to equal" in {
      val data = Array[Short](0x7E).map(_.toByte)
JefConverter.convJefToUtfHalf(data) mustBe "="
      }
"0x7F half char to double quote" in {
      val data = Array[Short](0x7F).map(_.toByte)
JefConverter.convJefToUtfHalf(data) mustBe "\""
      }
"illegal data pattern　half" in {
      val data = Array[Short](0xF1, 0x00, 0xFF, 0xF2).map(_.toByte)
JefConverter.convJefToUtfHalf(data) mustBe "1 *2"
      }
"all null data" in {
      val data = Array[Short](0x00, 0x00, 0x00, 0x00).map(_.toByte)
JefConverter.convJefToUtfHalf(data) mustBe "    "
      }
   }

   import JefConverter.implicits._

   "UTF8 to JEF Full" should {
   "be normal end." in {
      val binData = Array[Short](0xA3, 0xCA, 0x40, 0x40, 0xA3, 0xC5, 0x40, 0x40, 0xA3, 0xC6).map(_.toByte).map( x =>f"$x%02x")
"Ｊ　Ｅ　Ｆ".toJefFull.map( x =>f"$x%02x") mustBe binData
      }
   }

   "UTF8 to JEF Half" should {
   "be normal end." in {
      val binData = Array[Short](0xD1, 0x40, 0xC5, 0x40, 0xC6).map(_.toByte).map( x =>f"$x%02x")
"J E F".toJefHalf.map( x =>f"$x%02x") mustBe binData
      }
"check tab" in {
      val binData = Array[Short](0xD1, 0x05, 0xC5, 0x05, 0xC6).map(_.toByte).map( x =>f"$x%02x")
"J\tE\tF".toJefHalf.map( x =>f"$x%02x") mustBe binData
      }
   }
}