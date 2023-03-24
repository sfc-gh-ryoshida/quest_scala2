package d2k.common.fileConv

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter

class DomainProcessorTest extends WordSpec with MustMatchers with BeforeAndAfter {
  "format test" should {
    "YYYYMMDD" in {
      val domain_name = "年月日"
      val empty = "        "
      val invalid = "20151232"
      val max = "99991231"
      val max_over = "99991232"
      val min = "00010101"
      val min_under = "00010100"
      val not_digit = "0001010 "
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "YYYYMMDD_SLASH" in {
      val domain_name = "年月日_SL"
      val empty = "          "
      val invalid = "2015/12/32"
      val max = "9999/12/31"
      val max_over = "9999/12/32"
      val min = "0001/01/01"
      val min_under = "0001/01/00"
      val not_digit = "0001/01/0 "
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "YYYYMMDD_Hyphen" in {
      val domain_name = "年月日_HY"
      val empty = "          "
      val invalid = "2015-12-32"
      val max = "9999-12-31"
      val max_over = "9999-12-32"
      val min = "0001-01-01"
      val min_under = "0001-01-00"
      val not_digit = "0001-01-0 "
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "YYYYMM" in {
      val domain_name = "年月"
      val empty = "      "
      val invalid = "201513"
      val max = "999912"
      val max_over = "999913"
      val min = "000101"
      val min_under = "000100"
      val not_digit = "00010 "
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "MMDD" in {
      val domain_name = "月日"
      val empty = "    "
      val invalid = "1032"
      val max = "1231"
      val max_over = "1232"
      val min = "0101"
      val min_under = "0100"
      val not_digit = "010 "
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "YYYYMMDDHHMMSS" in {
      val domain_name = "年月日時分秒"
      val empty = "              "
      val invalid = "20151231235960"
      val max = "99991231235959"
      val max_over = "99991231235960"
      val min = "00010101000000"
      val min_under = "00010100000000"
      val not_digit = "0001010000000 "
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "年月日時分秒_HC" in {
      val domain_name = "年月日時分秒_HC"
      execRight(domain_name, "2016-06-01 12:34:56") mustBe "20160601123456"

      execRight(domain_name, "                   ") mustBe "00010101000000"
      execRight(domain_name, "") mustBe "00010101000000"
      execRight(domain_name, "2016-6-1 12:34:56") mustBe "00010101000000"
    }

    "年月日時分秒_SC" in {
      val domain_name = "年月日時分秒_SC"
      execRight(domain_name, "2016/06/01 12:34:56") mustBe "20160601123456"

      execRight(domain_name, "                   ") mustBe "00010101000000"
      execRight(domain_name, "") mustBe "00010101000000"
      execRight(domain_name, "2016/6/1 12:34:56") mustBe "00010101000000"
    }

    "YYYYMMDDHHMMSSMS" in {
      val domain_name = "年月日時分ミリ秒"
      val empty = "                 "
      val invalid = "20151231235960999"
      val max = "99991231235959999"
      val max_over = "99991231235960999"
      val min = "00010101000000000"
      val min_under = "00010101000000000"
      val not_digit = "0001010100000000 "
      val max_exp = "99991231235959999"
      val min_exp = "00010101000000000"
      timestampTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid, max_exp, min_exp)
    }

    "年月日時分ミリ秒/ミリ秒小数点付加" in {
      val domain_name = "年月日時分ミリ秒/ミリ秒小数点付加"
      execRight(domain_name, "20160601123456123") mustBe "20160601123456.123"

      execRight(domain_name, "                   ") mustBe "00010101000000.000"
      execRight(domain_name, "") mustBe "00010101000000.000"
      execRight(domain_name, "2016/6/1 12:34:56") mustBe "00010101000000.000"
    }

    "YYYYMMDDHHMMSSMS_len15" in {
      val domain_name = "年月日時分ミリ秒"
      val empty = "               "
      val invalid = "201512312359609"
      val max = "999912312359599"
      val max_over = "999912312359609"
      val min = "000101010000000"
      val min_under = "000101010000000"
      val not_digit = "00010101000000 "
      val max_exp = "99991231235959900"
      val min_exp = "00010101000000000"
      timestampTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid, max_exp, min_exp)
    }

    "年月日時分ミリ秒/ミリ秒小数点付加 len15" in {
      val domain_name = "年月日時分ミリ秒/ミリ秒小数点付加"
      execRight(domain_name, "201606011234561") mustBe "20160601123456.100"

      execRight(domain_name, "                 ") mustBe "00010101000000.000"
      execRight(domain_name, "") mustBe "00010101000000.000"
      execRight(domain_name, "2016/6/1 12:34:56") mustBe "00010101000000.000"
    }

    "YYYYMMDDHHMMSSMS_len16" in {
      val domain_name = "年月日時分ミリ秒"
      val empty = "                "
      val invalid = "2015123123596099"
      val max = "9999123123595999"
      val max_over = "9999123123596099"
      val min = "0001010100000000"
      val min_under = "0001010100000000"
      val not_digit = "000101010000000 "
      val max_exp = "99991231235959990"
      val min_exp = "00010101000000000"
      timestampTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid, max_exp, min_exp)
    }

    "年月日時分ミリ秒/ミリ秒小数点付加 len16" in {
      val domain_name = "年月日時分ミリ秒/ミリ秒小数点付加"
      execRight(domain_name, "2016060112345612") mustBe "20160601123456.120"

      execRight(domain_name, "                  ") mustBe "00010101000000.000"
      execRight(domain_name, "") mustBe "00010101000000.000"
      execRight(domain_name, "2016/6/1 12:34:56") mustBe "00010101000000.000"
    }

    "YYYYMMDDHH" in {
      val domain_name = "年月日時"
      val empty = "          "
      val invalid = "2015123124"
      val max = "9999123123"
      val max_over = "9999123124"
      val min = "0001010100"
      val min_under = "0001010000"
      val not_digit = "000101000 "
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "HHMMSS" in {
      val domain_name = "時分秒"
      val empty = "      "
      val invalid = "225960"
      val max = "235959"
      val max_over = "235960"
      val min = "000000"
      val min_under = null
      val not_digit = "00000 "
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "HHMMSS_COLON" in {
      val domain_name = "時分秒_CO"
      val empty = "      "
      val invalid = "22:59:60"
      val max = "23:59:59"
      val max_over = "23:59:60"
      val min = "00:00:00"
      val min_under = null
      val not_digit = "00:00:0 "
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "HHMMSSMS" in {
      val domain_name = "時分ミリ秒"
      val empty = "         "
      val invalid = "225960999"
      val max = "235959990"
      val max_over = "235960999"
      val min = "000000000"
      val min_under = null
      val not_digit = "00000000 "
      val max_exp = "235959990"
      val min_exp = "000000000"
      timestampTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid, max_exp, min_exp)
    }

    "時分ミリ秒/ミリ秒小数点付加" in {
      val domain_name = "時分ミリ秒/ミリ秒小数点付加"
      execRight(domain_name, "123456123") mustBe "123456.123"

      execRight(domain_name, "                   ") mustBe "000000.000"
      execRight(domain_name, "") mustBe "000000.000"
      execRight(domain_name, "2016/6/1 12:34:56") mustBe "000000.000"
    }

    "HHMMSSMS_len8" in {
      val domain_name = "時分ミリ秒"
      val empty = "        "
      val invalid = "22596099"
      val max = "23595990"
      val max_over = "23596099"
      val min = "00000000"
      val min_under = null
      val not_digit = "0000000 "
      val max_exp = "235959900"
      val min_exp = "000000000"
      timestampTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid, max_exp, min_exp)
    }

    "時分ミリ秒/ミリ秒小数点付加　len8" in {
      val domain_name = "時分ミリ秒/ミリ秒小数点付加"
      execRight(domain_name, "1234561") mustBe "123456.100"

      execRight(domain_name, "                 ") mustBe "000000.000"
      execRight(domain_name, "") mustBe "000000.000"
      execRight(domain_name, "2016/6/1 12:34:56") mustBe "000000.000"
    }

    "HHMMSSMS_len7" in {
      val domain_name = "時分ミリ秒"
      val empty = "       "
      val invalid = "2259609"
      val max = "2359599"
      val max_over = "2359609"
      val min = "0000000"
      val min_under = null
      val not_digit = "000000 "
      val max_exp = "235959900"
      val min_exp = "000000000"
      timestampTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid, max_exp, min_exp)
    }

    "時分ミリ秒/ミリ秒小数点付加　len7" in {
      val domain_name = "時分ミリ秒/ミリ秒小数点付加"
      execRight(domain_name, "12345612") mustBe "123456.120"

      execRight(domain_name, "                  ") mustBe "000000.000"
      execRight(domain_name, "") mustBe "000000.000"
      execRight(domain_name, "2016/6/1 12:34:56") mustBe "000000.000"
    }

    "HHMM" in {
      val domain_name = "時分"
      val empty = "    "
      val invalid = "2260"
      val max = "2359"
      val max_over = "2400"
      val min = "0000"
      val min_under = null
      val not_digit = "000 "
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "YYYY" in {
      val domain_name = "年"
      val empty = "     "
      val max = "9999"
      val max_over = "10000"
      val min = "0001"
      val min_under = "0000"
      val not_digit = "000 "
      val invalid = null
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "MM" in {
      val domain_name = "月"
      val empty = "  "
      val max = "12"
      val max_over = "13"
      val min = "01"
      val min_under = "00"
      val not_digit = "0 "
      val invalid = null
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "DD" in {
      val domain_name = "日"
      val empty = "  "
      val max = "31"
      val max_over = "32"
      val min = "01"
      val min_under = "00"
      val not_digit = "0 "
      val invalid = null
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "HH" in {
      val domain_name = "時"
      val empty = "  "
      val max = "23"
      val max_over = "24"
      val min = "00"
      val min_under = null
      val not_digit = "0 "
      val invalid = null
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "MI" in {
      val domain_name = "分"
      val empty = "  "
      val max = "59"
      val max_over = "60"
      val min = "00"
      val min_under = null
      val not_digit = "0 "
      val invalid = null
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "SS" in {
      val domain_name = "秒"
      val empty = "  "
      val max = "59"
      val max_over = "60"
      val min = "00"
      val min_under = null
      val not_digit = "0 "
      val invalid = null
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid)
    }

    "TIME" in {
      val domain_name = "時間"
      val empty = "      "
      val invalid = "985960"
      val max = "995959"
      val max_over = "995960"
      val min = "000000"
      val min_under = null
      val not_digit = "00000 "
      dateTest(domain_name, empty, max, max_over, min, min_under, not_digit, invalid, DomainProcessor.ERR_MSG_INVALID_VALUE)
    }

    "success 年月_SL convert" in {
      val domain_name = "年月_SL"
      execRight(domain_name, "2016/01") mustBe "201601"
      execRight(domain_name, "0000/00") mustBe "000101"
      execRight(domain_name, "9999/99") mustBe "999912"
    }

    "success 年月日時分 convert" in {
      val domain_name = "年月日時分"
      execRight(domain_name, "201601020304") mustBe "201601020304"
      execRight(domain_name, "000000000000") mustBe "000101010000"
      execRight(domain_name, "999999999999") mustBe "999912312359"
    }

    "success 年月日時分_SC convert" in {
      val domain_name = "年月日時分_SC"
      execRight(domain_name, "2016/01/02 03:04") mustBe "201601020304"
      execRight(domain_name, "0000/00/00 00:00") mustBe "000101010000"
      execRight(domain_name, "9999/99/99 99:99") mustBe "999912312359"
    }

    "success 年月日時分秒_CO convert" in {
      val domain_name = "年月日時分秒_CO"
      execRight(domain_name, "20160102 03:04:05") mustBe "20160102030405"
      execRight(domain_name, "00000000 00:00:00") mustBe "00010101000000"
      execRight(domain_name, "99999999 99:99:99") mustBe "99991231235959"
    }

    "PD_DATE" in {
      val domain_name = "年月日_PD"
      execRight2(domain_name, Array(0x00, 0x10, 0x00, 0x10, 0x1c)) mustBe "01000101"
    }

    "PD_DATE2" in {
      val domain_name = "年月日_PD"
      execRight2(domain_name, Array(0x01, 0x00, 0x70, 0x10, 0x1c)) mustBe "10070101"
      execRight2(domain_name, Array(0x01, 0x00, 0x80, 0x10, 0x1c)) mustBe "10080101"

      val null_exp = execRight2(domain_name, Array(0x00, 0x00))
      null_exp mustBe "00010101"
    }

    "PD" in {
      val domain_name = "数字_PD"
      val nullStr = new String(Array[Byte](0x00, 0x00), "MS932")
      val null_exp = execRight2(domain_name, Array(0x00, 0x00))
      println(null_exp)
      null_exp mustBe nullStr

      val minus = execRight2(domain_name, Array(0x00, 0x01, 0x20, 0x00, 0x3d))
      minus mustBe "-120003"

      val minus2 = execRight2(domain_name, Array(0x00, 0x01, 0x20, 0x0d))
      minus2 mustBe "-1200"

      val plus = execRight2(domain_name, Array(0x21, 0x3c))
      plus mustBe "213"

      val empty = execLeft2(domain_name, Array(0x20, 0x20))
      empty mustBe DomainProcessor.ERR_MSG_INVALID_VALUE

      val invalidSign = execLeft2(domain_name, Array(0x20, 0x00))
      invalidSign mustBe DomainProcessor.ERR_MSG_INVALID_VALUE

      val notDigit = execLeft2(domain_name, Array(0x0f, 0x0d))
      notDigit mustBe DomainProcessor.ERR_MSG_INVALID_VALUE
    }

    "文字列_PD" in {
      val domain_name = "文字列_PD"
      execRight2(domain_name, Array(0x21, 0x3c)) mustBe "213"
      execRight2(domain_name, Array(0x02, 0x3c)) mustBe "023"
      execRight2(domain_name, Array(0x00, 0x21, 0x3c)) mustBe "00213"
      execRight2(domain_name, Array(0x01, 0x20, 0x0d)) mustBe "-01200"
      execRight2(domain_name, Array(0x00, 0x01, 0x20, 0x00, 0x3d)) mustBe "-000120003"
    }

    "識別子_PD" in {
      val domain_name = "識別子_PD"
      execRight2(domain_name, Array(0x21, 0x3c)) mustBe "213"
      execRight2(domain_name, Array(0x02, 0x3c)) mustBe "023"
      execRight2(domain_name, Array(0x00, 0x21, 0x3c)) mustBe "00213"
      execRight2(domain_name, Array(0x01, 0x20, 0x0d)) mustBe "01200"
      execRight2(domain_name, Array(0x00, 0x01, 0x20, 0x00, 0x3d)) mustBe "000120003"
    }

    "ZD" in {
      val domain_name = "数字_ZD"
      val nullStr = new String(Array[Byte](0x00, 0x00), "MS932")
      val null_exp = execRight2(domain_name, Array(0x00, 0x00))
      null_exp mustBe nullStr

      val minus = execRight2(domain_name, Array(0xf1, 0xf2, 0xf0, 0xf0, 0xf0, 0xd3))
      minus mustBe "-120003"

      val plus = execRight2(domain_name, Array(0xf2, 0xf1, 0xf3))
      plus mustBe "213"

      val plus2 = execRight2(domain_name, Array(0xf2, 0xf1, 0xc3))
      plus2 mustBe "213"

      val empty = execLeft2(domain_name, Array(0x40, 0x40))
      empty mustBe DomainProcessor.ERR_MSG_INVALID_VALUE

      val invalidSign = execLeft2(domain_name, Array(0x40, 0x00))
      invalidSign mustBe DomainProcessor.ERR_MSG_INVALID_VALUE

      val notDigit = execLeft2(domain_name, Array(0x0f, 0x0d))
      notDigit mustBe DomainProcessor.ERR_MSG_INVALID_VALUE
    }

    "文字列_ZD" in {
      val domain_name = "文字列_ZD"
      execRight2(domain_name, Array(0xf2, 0xf1, 0xc3)) mustBe "213"
      execRight2(domain_name, Array(0xf0, 0xf2, 0xf1, 0xc3)) mustBe "0213"
      execRight2(domain_name, Array(0xf0, 0xf0, 0xf2, 0xf1, 0xc3)) mustBe "00213"
      execRight2(domain_name, Array(0xf1, 0xf2, 0xf0, 0xf0, 0xf0, 0xd3)) mustBe "-120003"
      execRight2(domain_name, Array(0xf0, 0xf1, 0xf2, 0xf0, 0xf0, 0xf0, 0xd3)) mustBe "-0120003"
      execRight2(domain_name, Array(0xf0, 0xf0, 0xf1, 0xf2, 0xf0, 0xf0, 0xf0, 0xd3)) mustBe "-00120003"
    }

    "識別子_ZD" in {
      val domain_name = "識別子_ZD"
      execRight2(domain_name, Array(0xf2, 0xf1, 0xc3)) mustBe "213"
      execRight2(domain_name, Array(0xf0, 0xf2, 0xf1, 0xc3)) mustBe "0213"
      execRight2(domain_name, Array(0xf0, 0xf0, 0xf2, 0xf1, 0xc3)) mustBe "00213"
      execRight2(domain_name, Array(0xf1, 0xf2, 0xf0, 0xf0, 0xf0, 0xd3)) mustBe "120003"
      execRight2(domain_name, Array(0xf0, 0xf1, 0xf2, 0xf0, 0xf0, 0xf0, 0xd3)) mustBe "0120003"
      execRight2(domain_name, Array(0xf0, 0xf0, 0xf1, 0xf2, 0xf0, 0xf0, 0xf0, 0xd3)) mustBe "00120003"
    }

    "Digit" in {
      val domain_name = "数字"

      val empty = execLeft2(domain_name, Array(0x20, 0x20))
      empty mustBe DomainProcessor.ERR_MSG_INVALID_VALUE

      val notDigit = execLeft(domain_name, "AA")
      notDigit mustBe DomainProcessor.ERR_MSG_INVALID_VALUE

      val nullStr = new String(Array[Byte](0x00, 0x00), "MS932")
      val null_exp = execRight(domain_name, nullStr)
      null_exp mustBe nullStr

    }

    "数字_SIGNED" in {
      val domain_name = "数字_SIGNED"
      execRight(domain_name, "+0") mustBe "0"
      execRight(domain_name, "-0") mustBe "0"
      execRight(domain_name, "+10000") mustBe "10000"
      execRight(domain_name, "-10000") mustBe "-10000"
      execRight(domain_name, "+00001") mustBe "1"
      execRight(domain_name, "-00001") mustBe "-1"
      execRight(domain_name, " 0") mustBe "0"
      execRight(domain_name, "-A") mustBe "0"
      execRight(domain_name, "  ") mustBe "0"
    }

    "文字列" in {
      val domain_name = "文字列"
      execRight(domain_name, "AB") mustBe "AB"
      execRight(domain_name, "AB  ") mustBe "AB"
      execRight(domain_name, "  AB") mustBe "AB"
      execRight(domain_name, "  AB  ") mustBe "AB"
      execRight(domain_name, "20151232") mustBe "20151232"

      //半角
      execRight(domain_name, "AB  ") mustBe "AB"
      execRight(domain_name, "  AB") mustBe "AB"
      execRight(domain_name, "  AB  ") mustBe "AB"
      //全角
      execRight(domain_name, "AB　　") mustBe "AB　　"
      execRight(domain_name, "　　AB") mustBe "　　AB"
      execRight(domain_name, "　　AB　　") mustBe "　　AB　　"
      //全半角混在
      execRight(domain_name, "　 　 AB　　") mustBe "　 　 AB　　"
      execRight(domain_name, "　 　 AB") mustBe "　 　 AB"
      execRight(domain_name, " 　 　 AB　 　 ") mustBe "　 　 AB　 　"
      execRight(domain_name, "20151232") mustBe "20151232"
    }

    "文字列_trim_無し" in {
      val domain_name = "文字列_trim_無し"
      execRight(domain_name, "AB") mustBe "AB"

      //半角
      execRight(domain_name, "AB  ") mustBe "AB  "
      execRight(domain_name, "  AB") mustBe "  AB"
      execRight(domain_name, "  AB  ") mustBe "  AB  "
      //全角
      execRight(domain_name, "AB　　") mustBe "AB　　"
      execRight(domain_name, "　　AB") mustBe "　　AB"
      execRight(domain_name, "　　AB　　") mustBe "　　AB　　"
      //全半角混在
      execRight(domain_name, "　 　 AB　　") mustBe "　 　 AB　　"
      execRight(domain_name, "　 　 AB") mustBe "　 　 AB"
      execRight(domain_name, " 　 　 AB　 　 ") mustBe " 　 　 AB　 　 "
      execRight(domain_name, "20151232") mustBe "20151232"
    }

    "文字列_trim_半角" in {
      val domain_name = "文字列_trim_半角"
      execRight(domain_name, "AB") mustBe "AB"
      execRight(domain_name, "AB  ") mustBe "AB"
      execRight(domain_name, "  AB") mustBe "AB"
      execRight(domain_name, "  AB  ") mustBe "AB"
      execRight(domain_name, "20151232") mustBe "20151232"

      //半角
      execRight(domain_name, "AB  ") mustBe "AB"
      execRight(domain_name, "  AB") mustBe "AB"
      execRight(domain_name, "  AB  ") mustBe "AB"
      //全角
      execRight(domain_name, "AB　　") mustBe "AB　　"
      execRight(domain_name, "　　AB") mustBe "　　AB"
      execRight(domain_name, "　　AB　　") mustBe "　　AB　　"
      //全半角混在
      execRight(domain_name, "　 　 AB　　") mustBe "　 　 AB　　"
      execRight(domain_name, "　 　 AB") mustBe "　 　 AB"
      execRight(domain_name, " 　 　 AB　 　 ") mustBe "　 　 AB　 　"
      execRight(domain_name, "20151232") mustBe "20151232"
    }

    "文字列_trim_全角" in {
      val domain_name = "文字列_trim_全角"
      execRight(domain_name, "AB") mustBe "AB"
      execRight(domain_name, "AB　　") mustBe "AB"
      execRight(domain_name, "　　AB") mustBe "AB"
      execRight(domain_name, "　　AB　　") mustBe "AB"
      execRight(domain_name, "20151232") mustBe "20151232"

      //半角
      execRight(domain_name, "AB  ") mustBe "AB  "
      execRight(domain_name, "  AB") mustBe "  AB"
      execRight(domain_name, "  AB  ") mustBe "  AB  "
      //全角
      execRight(domain_name, "AB　　") mustBe "AB"
      execRight(domain_name, "　　AB") mustBe "AB"
      execRight(domain_name, "　　AB　　") mustBe "AB"
      //全半角混在
      execRight(domain_name, "　 　 AB　　") mustBe " 　 AB"
      execRight(domain_name, "　 　 AB") mustBe " 　 AB"
      execRight(domain_name, " 　 　 AB　 　 ") mustBe " 　 　 AB　 　 "
      execRight(domain_name, "20151232") mustBe "20151232"
    }

    "文字列_trim_全半角" in {
      val domain_name = "文字列_trim_全半角"
      //半角
      execRight(domain_name, "AB  ") mustBe "AB"
      execRight(domain_name, "  AB") mustBe "AB"
      execRight(domain_name, "  AB  ") mustBe "AB"
      //全角
      execRight(domain_name, "AB　　") mustBe "AB"
      execRight(domain_name, "　　AB") mustBe "AB"
      execRight(domain_name, "　　AB　　") mustBe "AB"
      //全半角混在
      execRight(domain_name, "　 　 AB　　") mustBe "AB"
      execRight(domain_name, "　 　 AB") mustBe "AB"
      execRight(domain_name, " 　 　 AB　 　 ") mustBe "AB"
      execRight(domain_name, "20151232") mustBe "20151232"
    }

    "全角文字列" in {
      val domain_name = "全角文字列"
      execRight(domain_name, "あいう　　") mustBe "あいう"
      execRight(domain_name, "　　あいう") mustBe "あいう"
      execRight(domain_name, "　　あいう　　") mustBe "あいう"
    }

    "全角文字列_trim_無し" in {
      val domain_name = "全角文字列_trim_無し"
      //全角
      execRight(domain_name, "あいう　　") mustBe "あいう　　"
      execRight(domain_name, "　　あいう") mustBe "　　あいう"
      execRight(domain_name, "　　あいう　　") mustBe "　　あいう　　"
    }

    "全角文字列_trim_全角" in {
      val domain_name = "全角文字列_trim_全角"
      execRight(domain_name, "あいう　　") mustBe "あいう"
      execRight(domain_name, "　　あいう") mustBe "あいう"
      execRight(domain_name, "　　あいう　　") mustBe "あいう"
    }

    "DataDiv_ALPHABET" in {
      val domain_name = "レコード区分_ALPHABET"

      val head = execRight(domain_name, "H")
      head mustBe "H"

      val data = execRight(domain_name, "D")
      data mustBe "D"

      val foot = execRight(domain_name, "T")
      foot mustBe "T"

    }
    "DataDiv_NUMBER" in {
      val domain_name = "レコード区分_NUMBER"

      val head = execRight(domain_name, "1")
      head mustBe "H"

      val data = execRight(domain_name, "2")
      data mustBe "D"

      val foot = execRight(domain_name, "3")
      foot mustBe "T"

      try {
        execRight(domain_name, "4")
        fail
      } catch {
        case t: RuntimeException => t.getMessage mustBe s"${DomainProcessor.ERR_MSG_INVALID_DATA_DIV}:4"
        case t: Throwable        => fail
      }

    }

    "Communication Method NULL" in {
      val domain_name = "通信方式"

      val result = execRight(domain_name, "")
      result mustBe ""

    }

    "Communication Method Right Space" in {
      val domain_name = "通信方式"

      val result = execRight(domain_name, "ABC    ")
      result mustBe "A"

    }

    "Communication Method Left Space" in {
      val domain_name = "通信方式"

      val result = execRight(domain_name, "   ABC")
      result mustBe "A"

    }

    "InvalidDomain" in {
      val domain_name = "HOGE"
      try {
        execRight(domain_name, "hoge")
        fail
      } catch {
        case t: RuntimeException => t.getMessage mustBe s"${DomainProcessor.ERR_MSG_INVALID_DOMAIN}:${domain_name}"
        case t: Throwable        => fail
      }
    }

    /*
   * 日付系 共通
   */
    def dateTest(domain_name: String, empty: String, max: String, max_over: String, min: String, min_under: String, not_digit: String, invalid: String, INVALID_DATE_MSG: String = null) {
      val max_exp = max.replaceAll("[-:/]", "")
      val min_exp = min.replaceAll("[-:/]", "")

      val actual_empty = execRight(domain_name, empty)
      actual_empty mustBe min_exp

      val nullString = new String(Array[Byte](0x00))
      val actual_null = execRight(domain_name, empty.replaceAll(" ", nullString))
      actual_null mustBe min_exp

      val actual_max = execRight(domain_name, max)
      actual_max mustBe max_exp

      val actual_min = execRight(domain_name, min)
      actual_min mustBe min_exp

      if (min_under != null) {
        val actual_min_under = execRight(domain_name, min_under)
        actual_min_under mustBe min_exp
      }
    }

    "Byte配列" should {
      "ISO-8859-1の文字列として格納されている" in {
        val domainName = "Byte配列"
        execRight(domainName, "あいう").getBytes("ISO-8859-1") mustBe "あいう".getBytes("ISO-8859-1")
        execRight2(domainName, Array(0x01, 0x02, 0x03)).getBytes("ISO-8859-1") mustBe Array(0x01, 0x02, 0x03)
      }
    }

    /*
   * タイムスタンプ
   */
    def timestampTest(domain_name: String, empty: String, max: String, max_over: String, min: String, min_under: String, not_digit: String, invalid: String, max_exp: String, min_exp: String) {

      val actual_empty = execRight(domain_name, empty)
      actual_empty mustBe min_exp

      val nullString = new String(Array[Byte](0x00))
      val actual_null = execRight(domain_name, empty.replaceAll(" ", nullString))
      actual_null mustBe min_exp

      val actual_max = execRight(domain_name, max)
      actual_max mustBe max_exp

      val actual_min = execRight(domain_name, min)
      actual_min mustBe min_exp

      if (min_under != null) {
        val actual_min_under = execRight(domain_name, min_under)
        actual_min_under mustBe min_exp
      }

    }

    def execLeft(domain_name: String, data: String) =
      checkLeft(DomainProcessor.exec(domain_name, data))

    def execLeft2(domain_name: String, data: Array[Int], charEnc: String = "MS932") =
      checkLeft(DomainProcessor.execArrayByte(domain_name, data.map(_.toByte), charEnc))

    def checkLeft(result: Either[String, String]) =
      if (result.isLeft) {
        result.left.get
      } else {
        "UNEXPECTED RIGHT VALUE:" + result.right.get
      }

    def execRight(domain_name: String, data: String) =
      checkRight(DomainProcessor.exec(domain_name, data))

    def execRight2(domain_name: String, data: Array[Int], charEnc: String = "MS932") =
      checkRight(DomainProcessor.execArrayByte(domain_name, data.map(_.toByte), charEnc))

    def checkRight(result: Either[String, String]) =
      if (result.isRight) {
        result.right.get
      } else {
        "UNEXPECTED LEFT VALUE:" + result.left.get
      }
  }
}
