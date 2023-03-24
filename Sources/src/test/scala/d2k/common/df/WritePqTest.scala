package d2k.common.df

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.common.TestArgs
import d2k.common.InputArgs

import spark.common.SparkContexts.context.implicits._
import spark.common.PqCtl

case class WritePqData(a: String, b: String, c: String)
class WritePqTest extends WordSpec with MustMatchers with BeforeAndAfter {
  implicit val inArgs = TestArgs().toInputArgs
  "Single Write" should {
    "be success." in {

      val wq = new WritePq { val componentId = "writeaPqTest" }

      val df = Seq(WritePqData("a1", "b1", "c1"), WritePqData("a2", "b2", "c2")).toDF

      wq.writeParquet(df)

      val pqCtl = new PqCtl(wq.writePqPath)
      import pqCtl.implicits._
      val r = pqCtl.readParquet(wq.writePqName).as[WritePqData].collect

      r(0) mustBe WritePqData("a1", "b1", "c1")
      r(1) mustBe WritePqData("a2", "b2", "c2")

    }
  }

  "partition Write" should {
    "be success." in {

      val wq = new WritePq {
        val componentId = "writeaPqPartitionTest"
        override val writePqPartitionColumns = Seq("b", "c")
      }

      val df = Seq(
        WritePqData("a1", "b1", "c1"),
        WritePqData("a2", "b1", "c2"),
        WritePqData("a3", "b1", "c2"),
        WritePqData("a4", "b2", "c1"),
        WritePqData("a5", "b2", "c2"),
        WritePqData("a6", "b3", "c1"),
        WritePqData("a7", "b3", "c2")).toDF

      wq.writeParquet(df)

      val pqCtl = new PqCtl(wq.writePqPath)
      import pqCtl.implicits._

      val r1 = pqCtl.readParquet(s"${wq.writePqName}/b=b1/c=c1").as[String].collect.sorted
      r1.size mustBe 1
      r1(0) mustBe "a1"

      val r2 = pqCtl.readParquet(s"${wq.writePqName}/b=b1/c=c2").as[String].collect.sorted
      r2.size mustBe 2
      r2(0) mustBe "a2"
      r2(1) mustBe "a3"

      val r3 = pqCtl.readParquet(s"${wq.writePqName}/b=b3/c=c2").as[String].collect.sorted
      r3.size mustBe 1
      r3(0) mustBe "a7"

      val r4 = pqCtl.readParquet(s"${wq.writePqName}/b=b1").as[(String, String)].collect.sortBy(_._1)
      r4.size mustBe 3
      r4(0) mustBe ("a1", "c1")
      r4(1) mustBe ("a2", "c2")
      r4(2) mustBe ("a3", "c2")

      val r5 = pqCtl.readParquet(s"${wq.writePqName}/*/c=c1").as[String].collect.sorted
      r5.size mustBe 3
      r5(0) mustBe "a1"
      r5(1) mustBe "a4"
      r5(2) mustBe "a6"

      val r6 = pqCtl.readParquet(s"${wq.writePqName}/b=b1/*").as[String].collect.sorted
      r6.size mustBe 3
      r6(0) mustBe "a1"
      r6(1) mustBe "a2"
      r6(2) mustBe "a3"

      val r7 = pqCtl.readParquet(s"${wq.writePqName}/*/*").as[String].collect.sorted
      r7.size mustBe 7
      r7(0) mustBe "a1"
      r7(1) mustBe "a2"
      r7(2) mustBe "a3"
      r7(3) mustBe "a4"
      r7(4) mustBe "a5"
      r7(5) mustBe "a6"
      r7(6) mustBe "a7"
    }
  }
}