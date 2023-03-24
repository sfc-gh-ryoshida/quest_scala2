package d2k.appdefdoc.gen.test

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter

class GenerateTestCaseTest extends WordSpec with MustMatchers with BeforeAndAfter {
  "GenerateTestCase" should {
    "xxx" in {
      GenerateTestCase("http://10.47.148.28:8088/d2k_app_dev/d2k_docs")
        .generate("master", "sha", "SHAMCA0003001")
        .write()
    }
  }
}
