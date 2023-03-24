package d2k.common

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import java.sql.DriverManager
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 class HiRDBTest extends WordSpec with MustMatchers with BeforeAndAfter {
   "HiRDB" should {
   "connect db" in {
      Class.forName("JP.co.Hitachi.soft.HiRDB.JDBC.HiRDBDriver")
 val con = DriverManager.getConnection("jdbc:hitachi:hirdb://DBID=22200,DBHOST=localhost", "USER1", "USER1")
      }
   }
}