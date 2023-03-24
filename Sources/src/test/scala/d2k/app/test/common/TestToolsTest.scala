package d2k.app.test.common

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter

class TestToolsTest extends WordSpec with MustMatchers with BeforeAndAfter {
  "Domain Converter" should {
    "all space" in {
      val result = MakeDf.dc("test/dev/conf/import/projectId_items_test.conf").allSpace.collect.head
      result.getAs[String](0).size mustBe 0
      result.getAs[String](1).size mustBe 0
    }

    "all empty" in {
      val result = MakeDf.dc("test/dev/conf/import/projectId_items_test.conf").allEmpty.collect.head
      result.getAs[String](0).size mustBe 0
      result.getAs[String](1).size mustBe 0
    }
  }

  "Plain" should {
    "all space" in {
      val result = MakeDf("test/dev/conf/import/projectId_items_test.conf").allSpace.collect.head
      result.getAs[String](0).size mustBe 2
      result.getAs[String](1).size mustBe 3
    }

    "all empty" in {
      val result = MakeDf("test/dev/conf/import/projectId_items_test.conf").allEmpty.collect.head
      result.getAs[String](0).size mustBe 0
      result.getAs[String](1).size mustBe 0
    }
  }
}