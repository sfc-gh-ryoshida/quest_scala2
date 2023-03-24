package d2k.common

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter

class PostCodeNormalizerTest extends WordSpec with MustMatchers with BeforeAndAfter {
  "PostCodeNormalizer" should {
    "success convert" when {
      "Parent Child PostCode" in {
        (PostCodeNormalizer.parent("300"), PostCodeNormalizer.child("1")) mustBe ("300", "")
        (PostCodeNormalizer.parent("300"), PostCodeNormalizer.child("11")) mustBe ("300", "11")
        (PostCodeNormalizer.parent("300"), PostCodeNormalizer.child("111")) mustBe ("300", "")
        (PostCodeNormalizer.parent("300"), PostCodeNormalizer.child("1111")) mustBe ("300", "1111")
        (PostCodeNormalizer.parent("300"), PostCodeNormalizer.child("11111")) mustBe ("300", "")

        (PostCodeNormalizer.parent(""), PostCodeNormalizer.child("1")) mustBe ("", "")
        (PostCodeNormalizer.parent("3"), PostCodeNormalizer.child("11")) mustBe ("", "11")
        (PostCodeNormalizer.parent("30"), PostCodeNormalizer.child("111")) mustBe ("", "")
        (PostCodeNormalizer.parent("30"), PostCodeNormalizer.child("1111")) mustBe ("", "1111")
        (PostCodeNormalizer.parent("3001"), PostCodeNormalizer.child("1111")) mustBe ("", "1111")
        (PostCodeNormalizer.parent("30011"), PostCodeNormalizer.child("1111")) mustBe ("", "1111")
        (PostCodeNormalizer.parent("300111"), PostCodeNormalizer.child("1111")) mustBe ("", "1111")
        (PostCodeNormalizer.parent("3001111"), PostCodeNormalizer.child("1111")) mustBe ("", "1111")
      }

      "Single PostCode" in {
        PostCodeNormalizer("300-") mustBe "300"
        PostCodeNormalizer("300-1") mustBe "300"
        PostCodeNormalizer("300-11") mustBe "300-11"
        PostCodeNormalizer("300-111") mustBe "300"
        PostCodeNormalizer("300-1111") mustBe "300-1111"
        PostCodeNormalizer("300-11111") mustBe "300"
      }

      "Single PostCode no hyphen" in {
        PostCodeNormalizer("3") mustBe ""
        PostCodeNormalizer("30") mustBe ""
        PostCodeNormalizer("300") mustBe "300"
        PostCodeNormalizer("3001") mustBe "300"
        PostCodeNormalizer("30011") mustBe "30011"
        PostCodeNormalizer("300111") mustBe "300"
        PostCodeNormalizer("3001111") mustBe "3001111"
        PostCodeNormalizer("30011111") mustBe "300"
      }
    }
  }
}
