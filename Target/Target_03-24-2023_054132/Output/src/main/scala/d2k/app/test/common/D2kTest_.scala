package d2k.app.test.common

import scala.reflect.io.Path
import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import spark.common.DfCtl._
import spark.common.DfCtl.implicits._
import spark.common.SparkContexts
import SparkContexts.context.implicits._
import scala.io.Source
import com.snowflake.snowpark.DataFrame
import org.scalatest.BeforeAndAfter
import d2k.common.InputArgs
import d2k.common.SparkApp
import d2k.common.df.flow.OneInToOneOutForDf
import d2k.common.df.flow.OneInToMapOutForDf
import d2k.common.df.flow.TwoInToOneOutForDf
import java.io.FileNotFoundException
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait D2kJoinTest extends D2kTest {
   val app = D2kTest.dummyApp

   val apps: Seq[SparkApp]

   case class JT (setup: JT => Unit) (check: JT => Unit) (implicit inArgs: InputArgs) {
      val appName = app.toString.split('$').apply(0).split('.').toSeq.last

      val mdPath = s"${ appName }/JT"

      def readMdTable(name: String) = makeRes.readMdTable(name)

      "JT:" + appName should {
      "be success" in {
         setup(this)
apps.foreach(_.exec)
check(this)
         }
      }
   }
}
 object D2kTest {
   case class DummyDf (x: String)

   val emptyDf = SparkContexts.context.emptyDataFrame

   val dummyApp = new SparkApp {
   def exec(implicit inArgs: InputArgs) = emptyDf
   }
}
trait D2kTest extends WordSpec with MustMatchers with BeforeAndAfter {
   val app: SparkApp

   val outputPath: String = sys.env.getOrElse("FILE_INPUT_PATH_DEFAULT", "test/dev/data/output")

   val readMdPath: String = "test/markdown"

   before{clean}

   def clean = {
   val path = Path(outputPath)
path.createDirectory(true, false)
   }

   import D2kTest._

   val makeRes = d2k.common.MakeResource(outputPath, readMdPath)

   case class FileInfo (path: Path, inMdData: makeRes.MdInfo, outMdData: makeRes.MdInfo) {
      val fileName = path.name.drop(4)

      val splitted = fileName.split('.')

      def no = path.name.take(3)

      def io = splitted(1)

      def name = splitted(0)
   }

   case class AT (setup: AT => Unit) (check: AT => Unit) (implicit inArgs: InputArgs) {
      val appName = app.toString.split('$').apply(0).split('.').toSeq.last

      val mdPath = s"${ appName }/AT"

      def readMdTable(name: String) = makeRes.readMdTable(s"$mdPath/$name")

      "AT:" + appName should {
      "be success" in {
         setup(this)
app.exec
check(this)
         }
      }
   }

   case class CT (testCase: String) (implicit inArgs: InputArgs) {
      val appName = app.toString.split('$').apply(0).split('.').toSeq.last

      val mdPath = s"${ appName }/CT/${ testCase }"

      private [this] def readMdTableBase(prefixName: String)(name: String) = makeRes.readMdTable(s"$mdPath/${ prefixName }/${ name }.md")

      case class CTPre () {
         def readMdTable = readMdTableBase("pre")_
      }

      val ctpre = CTPre()

      def pre [IN](component: OneInToOneOutForDf[IN, _])(setup: CTPre => IN)(check: String) = {
      "CT:" + testCase should {
         "be success" when {
            "pre" in {
               ctpre.readMdTable(s"${ check }.md").checkDf(component.preExec(setup(ctpre)))
               }
            }
         }
      }

      def pre [IN](component: OneInToMapOutForDf[IN, _])(setup: CTPre => IN)(check: Map[String, String]) = {
      "CT:" + testCase should {
         "be success" when {
            "pre" in {
               val resultMap = component.preExec(setup(ctpre))
resultMap.keys.foreach( key =>ctpre.readMdTable(s"${ check(key) }.md").checkDf(resultMap(key)))
               }
            }
         }
      }

      def pre [IN](component: TwoInToOneOutForDf[IN, IN, _])(setup: CTPre => (IN, IN))(check: String) = {
      "CT:" + testCase should {
         "be success" when {
            "pre" in {
               val resultDfs = setup(ctpre)
ctpre.readMdTable(check).checkDf(component.preExec(resultDfs._1, resultDfs._2))
               }
            }
         }
      }

      case class CTPost[OUT] (resultDf: OUT) {
         def readMdTable = readMdTableBase("post")_
      }

      def postMdToDf(name: String) = readMdTableBase("post")(name).toDf

      def post [OUT](component: OneInToOneOutForDf[_, OUT])(setup: String)(check: CTPost[OUT] => Unit) = {
      "CT:" + testCase should {
         "be success" when {
            "post" in {
               check(CTPost(component.postExec(postMdToDf(setup))))
               }
            }
         }
      }

      def post [OUT](component: OneInToMapOutForDf[_, OUT])(setup: Map[String, String])(check: CTPost[OUT] => Unit) = {
      val mapDf = setup.mapValues{ name =>postMdToDf(name)}
"CT:" + testCase should {
         "be success" when {
            "post" in {
               check(CTPost(component.postExec(mapDf)))
               }
            }
         }
      }

      def post [OUT](component: TwoInToOneOutForDf[_, _, OUT])(setup: String)(check: CTPost[OUT] => Unit) = {
      "CT:" + testCase should {
         "be success" when {
            "post" in {
               check(CTPost(component.postExec(postMdToDf(setup))))
               }
            }
         }
      }
   }

   case class FT (componentName: String, testCase: String) {
      val appName = app.toString.split('$').apply(0).split('.').toSeq.last

      val mdPath = Path(s"${ readMdPath }/${ appName }/FT/${ componentName }")

      def apply(target: Seq[Editors]) = {
      val mapTarget = target.map( t =>(t.colName, t)).toMap
 val targetPath = (mdPath / testCase)
if (!targetPath.isDirectory)
            throw new FileNotFoundException (targetPath.toString)
 val fileInfos = targetPath.walk.map{ path => val mdStr = Source.fromFile(path.toString).mkString
 val splitted = mdStr.split("# expect")
FileInfo(path, makeRes.MdInfo(splitted(0).split("# input")(1)), makeRes.MdInfo(splitted(1)))
}
s"FT:${ componentName }:${ testCase }" should {
         "be success" when {
            fileInfos.foreach{ fi => val targetColumn = Option(mapTarget(fi.name)).flatMap{
               case e:Edit => Option(e.editor)
               case _ => None
               }.get
 val inputDf = if (fi.inMdData.data.replaceAll("\n", "").trim.isEmpty)
                  {
                  Seq(DummyDf("")).toDF
                  }
else
                  {
                  fi.inMdData.toDf
                  }
 val result = inputDf.select(targetColumn as fi.name)
s"${ fi.no }:${ fi.name }" in {
               withClue(fi.no){
                  val outputPos = fi.inMdData.data.count(_ == '\n')
fi.outMdData.checkDf(result, outputPos)
                  }
               }
}
            }
         }
      }

      def apply(target: DataFrame => DataFrame) = {
      val targetPath = (mdPath / testCase)
if (!targetPath.isDirectory)
            throw new FileNotFoundException (targetPath.toString)
 val fileInfos = targetPath.walk.map{ path => val mdStr = Source.fromFile(path.toString).mkString
 val splitted = mdStr.split("# expect")
FileInfo(path, makeRes.MdInfo(splitted(0).split("# input")(1)), makeRes.MdInfo(splitted(1)))
}
s"FT:${ componentName }:${ testCase }" should {
         "be success" when {
            fileInfos.foreach{ fi => val inputDf = if (fi.inMdData.data.replaceAll("\n", "").trim.isEmpty)
                  {
                  Seq(DummyDf("")).toDF
                  }
else
                  {
                  fi.inMdData.toDf
                  }
 val result = target(inputDf)
s"${ fi.no }:${ fi.name }" in {
               withClue(fi.no){
                  val outputPos = fi.inMdData.data.count(_ == '\n')
fi.outMdData.checkDf(result, outputPos)
                  }
               }
}
            }
         }
      }
   }
}