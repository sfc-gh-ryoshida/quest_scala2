package d2k.common

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class KanaConvTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "KanaConv" should {
   "be convert kana char" in {
      KanaConverter("アイウエオ ") mustBe "ｱｲｳｴｵ "
      }
"be convert kana char　for select" in {
      KanaConverter.select("アイウエオ ") mustBe "ｱｲｳｴｵ"
      }
   }
}