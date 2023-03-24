package d2k.common.fileConv

import com.snowflake.snowpark.functions._
import d2k.common.InputArgs
import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import spark.common.PqCtl
import scala.io.Source
import d2k.common.TestArgs
import scala.reflect.io.Directory
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class ConfParserTest extends WordSpec with MustMatchers with BeforeAndAfter {
   implicit val inputArgs = TestArgs().toInputArgs.copy(
fileConvInputFile = "data/test/conv/conf/mdb_app.conf")

   "test Normal" should {
   val confs = ConfParser.parse("data/test/conv/conf/mdb_app.conf").toSeq
 val conf = confs.find{ x =>x.appConf.AppId == "APP012"}.get
"appConf check" in {
      conf.appConf.AppName mustBe "固定長コンバータ"
conf.appConf.destSystem mustBe "ALICE"
conf.appConf.fileFormat mustBe "fixed"
conf.appConf.newline mustBe true
conf.appConf.header mustBe false
conf.appConf.footer mustBe false
conf.appConf.storeType mustBe "all"
conf.appConf.inputFiles mustBe "convFixed0?.dat"
conf.appConf.comment mustBe "備考"
 val tsvNewLine = confs.find{ x =>x.appConf.AppId == "APP002"}.get.appConf.newline
tsvNewLine mustBe false
 val csvNewLine = confs.find{ x =>x.appConf.AppId == "APP005"}.get.appConf.newline
tsvNewLine mustBe false
 val vsvNewLine = confs.find{ x =>x.appConf.AppId == "APP011"}.get.appConf.newline
tsvNewLine mustBe false
 val csvStrictNewLine = confs.find{ x =>x.appConf.AppId == "APP008"}.get.appConf.newline
tsvNewLine mustBe false
 val itemConfs = conf.itemConfs.toArray
 val itemConf1 = itemConfs(0)
itemConf1.itemId mustBe "rcd_div"
itemConf1.itemName mustBe "レコード区分"
itemConf1.length mustBe "2"
itemConf1.cnvType mustBe "文字列"
itemConf1.extractTarget mustBe false
itemConf1.comment mustBe "備考"
 val itemConf2 = itemConfs(1)
itemConf2.itemId mustBe "nw_service"
itemConf2.itemName mustBe "ＮＷサービス"
itemConf2.length mustBe "2"
itemConf2.cnvType mustBe "数字_PD"
itemConf2.extractTarget mustBe false
itemConf2.comment mustBe ""
      }
   }

   "read itemConf from resource" should {
   "app conf" in {
      val confs = ConfParser.parse("projectId_app.conf").toSeq
confs.head.appConf.AppId mustBe "res"
confs.head.appConf.inputFiles mustBe "res.dat"
      }
"item Conf" in {
      val confs = ConfParser.parseItemConf(Directory("data/xxx"), "projectId", "res").toSeq
{
         val r = confs(0)
r.itemId mustBe "xxx1"
         }
{
         val r = confs(1)
r.itemId mustBe "xxx2"
         }
      }
   }
}