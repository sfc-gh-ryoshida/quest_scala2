package d2k.common.fileConv

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class DomainProcessorJefTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "JEF 全角" should {
   "be normal end" in {
      val data = Array[Short](0xA3, 0xCA, 0x40, 0x40, 0xA3, 0xC5, 0x40, 0x40, 0xA3, 0xC6).map(_.toByte)
DomainProcessor.execArrayByte("全角文字列", data, "JEF").right.get mustBe "Ｊ　Ｅ　Ｆ"
      }
"全角 space" in {
      val data = Array[Short](0x40, 0x40).map(_.toByte)
DomainProcessor.execArrayByte("全角文字列", data, "JEF").right.get mustBe ""
      }
"全角 space trim無し" in {
      val data = Array[Short](0x40, 0x40).map(_.toByte)
DomainProcessor.execArrayByte("全角文字列_trim_無し", data, "JEF").right.get mustBe "　"
      }
"全角 trim全角" in {
      val data = Array[Short](0x40, 0x40).map(_.toByte)
DomainProcessor.execArrayByte("全角文字列_trim_全角", data, "JEF").right.get mustBe ""
      }
"null文字" in {
      val data = Array[Short](0x00, 0x00).map(_.toByte)
DomainProcessor.execArrayByte("全角文字列", data, "JEF").right.get mustBe "■"
      }
   }

   "JEF 半角" should {
   "年月日" in {
      val domain = "年月日"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0xf0, 0xf1, 0xf0, 0xf2).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "20160102"
      }
"年月日_SL" in {
      val domain = "年月日_SL"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0x61, 0xf0, 0xf1, 0x61, 0xf0, 0xf2).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "20160102"
      }
"年月日_HY" in {
      val domain = "年月日_HY"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0x60, 0xf0, 0xf1, 0x60, 0xf0, 0xf2).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "20160102"
      }
"年月" in {
      val domain = "年月"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0xf0, 0xf2).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "201602"
      }
"年月_SL" in {
      val domain = "年月_SL"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0x61, 0xf0, 0xf2).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "201602"
      }
"月日" in {
      val domain = "月日"
 val data = Array[Short](0xf0, 0xf1, 0xf0, 0xf2).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "0102"
      }
"年" in {
      val domain = "年"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "2016"
      }
"月" in {
      val domain = "月"
 val data = Array[Short](0xf0, 0xf1).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "01"
      }
"日" in {
      val domain = "日"
 val data = Array[Short](0xf0, 0xf2).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "02"
      }
"年月日時分秒" in {
      val domain = "年月日時分秒"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0xf0, 0xf1, 0xf0, 0xf2, 0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "20160102012345"
      }
"年月日時分秒_HC" in {
      val domain = "年月日時分秒_HC"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0x60, 0xf0, 0xf1, 0x60, 0xf0, 0xf2, 0x40, 0xf0, 0xf1, 0x7a, 0xf2, 0xf3, 0x7a, 0xf4, 0xf5).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "20160102012345"
      }
"年月日時分秒_SC" in {
      val domain = "年月日時分秒_SC"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0x61, 0xf0, 0xf1, 0x61, 0xf0, 0xf2, 0x40, 0xf0, 0xf1, 0x7a, 0xf2, 0xf3, 0x7a, 0xf4, 0xf5).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "20160102012345"
      }
"年月日時分秒_CO" in {
      val domain = "年月日時分秒_CO"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0x7a, 0xf0, 0xf1, 0x7a, 0xf0, 0xf2, 0x40, 0xf0, 0xf1, 0x7a, 0xf2, 0xf3, 0x7a, 0xf4, 0xf5).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "20160102012345"
      }
"年月日時分ミリ秒" in {
      val domain = "年月日時分ミリ秒"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0xf0, 0xf1, 0xf0, 0xf2, 0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "20160102012345678"
      }
"年月日時分ミリ秒/ミリ秒小数点付加" in {
      val domain = "年月日時分ミリ秒/ミリ秒小数点付加"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0xf0, 0xf1, 0xf0, 0xf2, 0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "20160102012345.678"
      }
"年月日時" in {
      val domain = "年月日時"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0xf0, 0xf1, 0xf0, 0xf2, 0xf0, 0xf1).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "2016010201"
      }
"年月日時分" in {
      val domain = "年月日時分"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0xf0, 0xf1, 0xf0, 0xf2, 0xf0, 0xf1, 0xf2, 0xf3).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "201601020123"
      }
"年月日時分_SC" in {
      val domain = "年月日時分_SC"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0x61, 0xf0, 0xf1, 0x61, 0xf0, 0xf2, 0x40, 0xf0, 0xf1, 0x7a, 0xf2, 0xf3).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "201601020123"
      }
"時分秒" in {
      val domain = "時分秒"
 val data = Array[Short](0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "012345"
      }
"時分秒_CO" in {
      val domain = "時分秒_CO"
 val data = Array[Short](0xf0, 0xf1, 0x7a, 0xf2, 0xf3, 0x7a, 0xf4, 0xf5).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "012345"
      }
"時分ミリ秒" in {
      val domain = "時分ミリ秒"
 val data = Array[Short](0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "012345678"
      }
"時分ミリ秒/ミリ秒小数点付加" in {
      val domain = "時分ミリ秒/ミリ秒小数点付加"
 val data = Array[Short](0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "012345.678"
      }
"時分" in {
      val domain = "時分"
 val data = Array[Short](0xf0, 0xf1, 0xf2, 0xf3).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "0123"
      }
"時" in {
      val domain = "時"
 val data = Array[Short](0xf0, 0xf1).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "01"
      }
"分" in {
      val domain = "分"
 val data = Array[Short](0xf0, 0xf2).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "02"
      }
"秒" in {
      val domain = "秒"
 val data = Array[Short](0xf0, 0xf3).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "03"
      }
"時間" in {
      val domain = "時間"
 val data = Array[Short](0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "123456"
      }
"数字" in {
      val domain = "数字"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0xf0, 0xf1, 0xf0, 0xf1).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "20160101"
      }
"数字_SIGNED" in {
      val domain = "数字_SIGNED"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0xf0, 0xf1, 0xf0, 0xf1).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "20160101"
      }
"文字列" in {
      val domain = "文字列"
 val data = Array[Short](0xd1, 0xc5, 0xc6).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "JEF"
      }
"文字列_trim_無し" in {
      val domain = "文字列_trim_無し"
 val data = Array[Short](0x40, 0xd1, 0x40, 0xc5, 0x40, 0xc6, 0x40).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe " J E F "
      }
"文字列_trim_半角" in {
      val domain = "文字列_trim_半角"
 val data = Array[Short](0x40, 0x8d, 0xbe, 0x40, 0x51, 0x40, 0x9f, 0x40).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "ｼﾞ ｪ ﾌ"
      }
"レコード区分_NUMBER" in {
      val domain = "レコード区分_NUMBER"
 val data = Array[Short](0xf1).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "H"
      }
"レコード区分_ALPHABET" in {
      val domain = "レコード区分_ALPHABET"
 val data = Array[Short](0xf2, 0xf0, 0xf1, 0xf6, 0xf0, 0xf1, 0xf0, 0xf1).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "20160101"
      }
"通信方式" in {
      val domain = "通信方式"
 val data = Array[Short](0xc1, 0xc2).map(_.toByte)
DomainProcessor.execArrayByte(domain, data, "JEF").right.get mustBe "A"
      }
   }
}