package d2k.common.df.component.cmn

import org.apache.spark.sql.DataFrame
import d2k.common.df.template.PqToVal
import d2k.common.InputArgs
import org.apache.spark.sql.Row
import d2k.common.df.executor._
import spark.common.DfCtl.implicits._
import spark.common.DfCtl._
import org.apache.spark.sql.functions._
import spark.common._
import SparkContexts.context.implicits._

object PostCodeConverter {
  val postDataMap =
    new PqToVal[Map[String, (String, String)]] with Nothing {
      val componentId = "MBA935.pq"
      def outputValue(rows: Array[Row]): Map[String, (String, String)] = {
        rows.map { row =>
          (row.getAs[String]("CD_ZIP7LEN").trim, (row.getAs[String]("CD_KENCD"), row.getAs[String]("CD_DEMEGRPCD")))
        }.toMap
      }
    }

  def apply()(implicit inArgs: InputArgs) = new PostCodeConverter
}

class PostCodeConverter(implicit inArgs: InputArgs) extends Serializable {
  import PostCodeConverter._
  private[this] val postMap = postDataMap.run(Unit)

  def localGovernmentCode(postCodeName1: String, postCodeName2: String = "")(outName1: String, outName2: String = "") =
    cnvLocalGovernmentCode(postCodeName1, postCodeName2)(outName1, outName2)

  private[this] def cnvLocalGovernmentCode(
    postCodeName1: String, postCodeName2: String)(outName1: String, outName2: String) = (df: DataFrame) => {

    val convertUdf = udf { (inPostCode: String) =>
      val postCode = Option(inPostCode).map(_.replaceAllLiterally("-", "").trim).getOrElse("")

      def code3 = postMap.get(postCode)

      def code5 = postMap.get(postCode).orElse {
        postMap.get(postCode.take(3))
      }

      def code7 = postMap.get(postCode).orElse {
        postMap.get(postCode.take(3) + "0000")
      }.orElse {
        postMap.get(postCode.take(3))
      }

      (postCode.size match {
        case 3 => code3
        case 5 => code5
        case 7 => code7
        case _ => None
      }).getOrElse(("", ""))
    }

    val postCodeCol = if (postCodeName2.isEmpty)
      col(postCodeName1)
    else
      concat(trim(col(postCodeName1)), col(postCodeName2))

    val df2 = df ~> editColumns(Seq(("_POSTCODES_", convertUdf(postCodeCol)).e))

    val outCol = if (outName2.isEmpty)
      Seq((outName1, $"_POSTCODES_._1").e)
    else
      Seq((outName1, $"_POSTCODES_._1").e, (outName2, $"_POSTCODES_._2").e)

    (df2 ~> editColumns(outCol))
      .drop("_POSTCODES_")
      .na.fill("", Seq(outName1, outName2))
      .na.replace(outName1, Map("" -> "99"))
      .na.replace(outName2, Map("" -> "999"))
  }
}