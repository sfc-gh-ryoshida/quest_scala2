package d2k.common.df.template.base

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.Column
import com.snowflake.snowpark.functions._
import d2k.common.InputArgs
import d2k.common.df.flow.TwoInToOneOutForDf
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait TwoDfJoinToAny[OUT] extends TwoInToOneOutForDf[DataFrame, DataFrame, OUT] {
   val joinType = "left_outer"

   def joinExprs(left: DataFrame, right: DataFrame): Column

   def select(left: DataFrame, right: DataFrame): Seq[Column]

   protected [this] def addColumnPrefix(name: String) = (df: DataFrame) =>{
   df.schema.map( x =>df(x.name) as s"${ name }_${ x.name }")
   }

   protected [this] def dropDuplicate(left: DataFrame, right: DataFrame) = {
   val lNames = left.schema.map(_.name).toSet
 val rNames = right.schema.map(_.name).toSet
 val diff = (rNames -- lNames).toSeq.map(col)
right.select(diff :_*)("*")
   }

   protected [TwoDfJoinToAny] def mergeDropDuplicate(left: DataFrame, right: DataFrame) = Seq(left("*"), dropDuplicate(left, right))

   protected [TwoDfJoinToAny] def mergeWithPrefix(left: DataFrame, right: DataFrame, name: String) = left("*") +: addColumnPrefix(name)(right)

   def preExec(left: DataFrame, right: DataFrame)(implicit inArgs: InputArgs) : DataFrame = {
   val joined = left.join(right, joinExprs(left, right), joinType)
joined.select(select(left, right).toArray :_*)
   }
}