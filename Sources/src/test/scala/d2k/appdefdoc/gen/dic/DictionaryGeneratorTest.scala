package d2k.appdefdoc.gen.dic

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.appdefdoc.gen.dic._
import scala.reflect.io.Directory
import scala.reflect.io.Path.string2path

class DictionaryGeneratorTest extends WordSpec with MustMatchers with BeforeAndAfter {
  "GenerateTestCase" should {
    "be normal end" in {
      GenerateDictionary("http://10.47.148.28:8088/d2k_app_dev/d2k_docs")
        .generate("master")
    }
  }
}