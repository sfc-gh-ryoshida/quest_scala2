package d2k.appdefdoc.gen.src

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.appdefdoc.gen.src._
import scala.reflect.io.Directory
import scala.reflect.io.Path.string2path
import d2k.appdefdoc.parser.AppDefParser
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class SourceGeneratorTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "generateItemConf" should {
   "be normal end" in {
      val baseUrl = "http://10.47.148.28:8088/d2k_app_dev/d2k_docs"
 val branch = "master"
 val appGroup = "mka"
 val appId = "MKA0690001001"
 val appdef = AppDefParser(baseUrl, branch, appGroup, appId).get
 val writeBase = s"data/srcGen/${ appGroup }"
 val writePath = Directory(writeBase)
writePath.createDirectory(true, false)
 val inputFiles = appdef.inputList.filter(_.srcType.toLowerCase.startsWith("file"))
SourceGenerator.generateItemConf(baseUrl, branch, appGroup, appId, writePath)(inputFiles)
      }
   }
}