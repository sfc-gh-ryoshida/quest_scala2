package d2k.common.df.template

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.common.df.CsvInfo

import d2k.common.TestArgs
import scala.util.Try
import d2k.common.df.template._
import d2k.common.df.executor.Nothing

class FileToDf_UTF8Test extends WordSpec with MustMatchers with BeforeAndAfter {
  implicit val inArgs = TestArgs().toInputArgs

  "read utf-8" should {
    "success" in {
      val compo = new FileToDf with Nothing {
        val componentId = "csv"
        override val fileInputInfo = CsvInfo(Set("csv_utf8.dat"), charSet = "UTF-8")
      }
      val df = compo.run(Unit)
      val r = df.collect
      r(0).getAs[String]("item1") mustBe "あ"
      r(0).getAs[String]("item2") mustBe "う1"
      r(1).getAs[String]("item1") mustBe "い"
      r(1).getAs[String]("item2") mustBe "え2"
      r(2).getAs[String]("item1") mustBe ""
      r(2).getAs[String]("item2") mustBe ""
    }
  }
}
