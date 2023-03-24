package d2k.common

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class MakeResourceTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "makeFixedData" should {
   "be normal end." in {
      val mr = MakeResource("test/dev/data/output")
mr.readMdTable("makeResourceTest/fixed.md").toFixed("output.fixed")
      }
   }

   "makeJefData" should {
   "be normal end." in {
      val mr = MakeResource("test/dev/data/output")
mr.readMdTable("makeResourceTest/jef.md").toJef("output.jef")
      }
   }
}