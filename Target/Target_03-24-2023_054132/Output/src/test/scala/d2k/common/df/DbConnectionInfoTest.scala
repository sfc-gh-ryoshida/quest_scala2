package d2k.common.df

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class DbConnectionInfoTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "DbConnectionInfoTest" should {
   "dm1" in {
      val dm1 = DbConnectionInfo.dm1
dm1.url mustBe "dm1_url"
dm1.user mustBe "dm1_user"
dm1.password mustBe "dm1_password"
      }
"dwh1" in {
      val dwh1 = DbConnectionInfo.dwh1
dwh1.url mustBe "dwh1_url"
dwh1.user mustBe "dwh1_user"
dwh1.password mustBe "dwh1_password"
      }
"dwh2" in {
      val dwh2 = DbConnectionInfo.dwh2
dwh2.url mustBe "dwh2_url"
dwh2.user mustBe "dwh2_user"
dwh2.password mustBe "dwh2_password"
      }
"bat1" in {
      val bat1 = DbConnectionInfo.bat1
bat1.url mustBe "bat1_url"
bat1.user mustBe "bat1_user"
bat1.password mustBe "bat1_password"
      }
"csp1" in {
      val csp1 = DbConnectionInfo.csp1
csp1.url mustBe "csp1_url"
csp1.user mustBe "csp1_user"
csp1.password mustBe "csp1_password"
      }
"hi1" in {
      val hi1 = DbConnectionInfo.hi1
hi1.url mustBe "hi1_url"
hi1.user mustBe "hi1_user"
hi1.password mustBe "hi1_password"
      }
"mth1" in {
      val mth1 = DbConnectionInfo.mth1
mth1.url mustBe "mth1_url"
mth1.user mustBe "mth1_user"
mth1.password mustBe "mth1_password"
      }
   }
}