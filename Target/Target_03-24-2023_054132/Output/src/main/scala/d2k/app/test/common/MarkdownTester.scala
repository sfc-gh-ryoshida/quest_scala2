package d2k.app.test.common

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import com.snowflake.snowpark.DataFrame
import scala.util.Try
import d2k.common.MakeResource
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait MarkdownTester extends WordSpec with MustMatchers with BeforeAndAfter {
   val showData = Try(sys.env("MarkdownTesterShowData")).map( d =>if (d == "true")
      true
else
      false).getOrElse(false)

   def execUt(componentInstanceName: String)(targets: (DataFrame => DataFrame)*) = {
   val classNames = targets.head.getClass.getName.split('$')
 val appName = classNames.head.split('.').last
 val makeRes = MakeResource("test/dev/data/output", s"${ appName }Test/ut/${ componentInstanceName }")
s"be success ${ componentInstanceName }" when {
      targets.foreach{ func => val funcName = func.getClass.getName.split('$').dropRight(1).takeRight(1).head
funcName in {
         val df = makeRes.readMdTable(s"${ funcName }_data.md").toDf
if (showData)
               println(s"[Input Data:${ componentInstanceName }:${ funcName }]");df.show(false)
 val expect = makeRes.readMdTable(s"${ funcName }_expect.md")
if (showData)
               println(s"[Expect Data:${ componentInstanceName }:${ funcName }]");expect.toDf.show(false)
 val result = func(df)
if (showData)
               println(s"[Result Data:${ componentInstanceName }:${ funcName }]");result.show(false)
withClue("Record Size Check"){
result.count mustBe expect.toDf.count
}
expect.checkDf(result)
         }
}
      }
   }
}