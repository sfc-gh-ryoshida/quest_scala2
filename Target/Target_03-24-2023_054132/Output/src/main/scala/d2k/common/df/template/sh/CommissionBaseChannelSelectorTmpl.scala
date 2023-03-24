package d2k.common.df.template.sh

import d2k.common.InputArgs
import d2k.common.SparkApp
import d2k.common.df._
import d2k.common.df.executor._
import d2k.common.df.template._
import d2k.common.df.flow.OneInToOneOutForDf
import d2k.common.df.template.base.DfToAny
import d2k.common.df.template.base.AnyToDf
import spark.common.SparkContexts
import spark.common.DfCtl._
import spark.common.DfCtl.implicits._
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.Row

/*EWI: SPRKSCL1142 => org.apache.spark.sql.types.LongType is not supported*/
import org.apache.spark.sql.types.LongType
import SparkContexts.context.implicits._
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object CommissionBaseChannelSelectorTmpl {
   val UniqueKey = "_uniqKey_"
}
case class CommissionBaseChannelSelectorInfo (DV_DISPODIV: String, DV_OUTOBJDIV: String, DV_TRICALCOBJDIV: String)
trait CommissionBaseChannelSelectorTmpl extends DfToAny[DataFrame] with AnyToDf[DataFrame] {
   import CommissionBaseChannelSelectorTmpl._

   val info: CommissionBaseChannelSelectorInfo

   val groupingKeys: Seq[String] = Seq(UniqueKey)

   def exec(df: DataFrame)(implicit inArgs: InputArgs) = if (groupingKeys.contains(UniqueKey))
      {
      (df ~> c03_DfToDf.run, broadcast(c01_DbToDf(info).run(Unit))) ~> c02_DfJoinToDf.run ~> c04_DfToDf.run
      }
else
      {
      (df, broadcast(c01_DbToDf(info).run(Unit))) ~> c02_DfJoinToDf.run
      }

   private [this] def c01_DbToDf(info: CommissionBaseChannelSelectorInfo) = new DbToDf with Executor {
      val componentId = "MAA300"

      override val columns = Array("DV_DISCRDIV", "CD_CHNLCD", "DV_OUTOBJDIV", "DV_TRICALCOBJDIV")

      override val readDbWhere = Array(s"DV_DISPODIV = '${ info.DV_DISPODIV }'")

      def invoke(df: DataFrame)(implicit inArgs: InputArgs) = df ~> f01

      def f01(implicit inArgs: InputArgs) = (_ : DataFrame).na.fill(" ")
   }

   private [this] val c02_DfJoinToDf = new DfJoinToDf with Executor {
      val componentId = "MAA300"

      override def joinExprs(left: DataFrame, right: DataFrame) = ((right("DV_DISCRDIV") === "01") and (left("CD_CHNLGRPCD") === right("CD_CHNLCD"))) or ((right("DV_DISCRDIV") === "02") and (left("CD_CHNLDETAILCD") === right("CD_CHNLCD")))

      override def select(left: DataFrame, right: DataFrame) = mergeWithPrefix(left, right, componentId)

      def invoke(df: DataFrame)(implicit inArgs: InputArgs) = df ~> f01 ~> f02 ~> f03 ~> f04

      def f01 = editColumns(Seq(
("DV_DISCRDIV", coalesce($"MAA300_DV_DISCRDIV", lit("00"))), 
("DV_OUTOBJDIV", coalesce($"MAA300_DV_OUTOBJDIV", lit(info.DV_OUTOBJDIV))), 
("DV_TRICALCOBJDIV", coalesce($"MAA300_DV_TRICALCOBJDIV", lit(info.DV_TRICALCOBJDIV)))).e)

      def f02 = selectMaxValue(groupingKeys, Seq($"DV_DISCRDIV".desc))

      def f03 = dropColumnPrefix(componentId)

      def f04 = editColumns(Seq("DV_DISCRDIV").d)
   }

   //add Uniq Key
   private [this] def c03_DfToDf = new DfToDf with Executor {
      def invoke(df: DataFrame)(implicit inArgs: InputArgs) = df ~> f01

      def f01 = (df: DataFrame) =>{
      val newRdd = df.rdd.zipWithUniqueId.map{
         case (row, idx) => Row.fromSeq(row.toSeq :+ idx)
         }
SparkContexts.context.createDataFrame(newRdd, df.schema.add(UniqueKey, LongType))
      }
   }

   //drop Uniq Key
   private [this] def c04_DfToDf = new DfToDf with Executor {
      def invoke(df: DataFrame)(implicit inArgs: InputArgs) = df ~> f01

      def f01 = editColumns(Seq(UniqueKey).d)
   }
}