package d2k.common.df.executor

import d2k.common.df.Executor
import com.snowflake.snowpark.DataFrame
import d2k.common.InputArgs
import d2k.common.fileConv.ConfParser
import scala.reflect.io.Path
import d2k.common.fileConv.Converter
import com.snowflake.snowpark.Row
import spark.common.SparkContexts
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait BinaryRecordConverter extends Executor {
   val binaryRecordName: String

   val itemConfId: String

   val charEnc: String

   def invoke(df: DataFrame)(implicit inArgs: InputArgs) : DataFrame = BinaryRecordConverter(binaryRecordName, itemConfId, charEnc)(df)
}
 object BinaryRecordConverter {
   def apply(binaryRecordName: String, itemConfId: String, charEnc: String)(df: DataFrame)(implicit inArgs: InputArgs) : DataFrame = {
   val itemConfs = ConfParser.parseItemConf(Path(inArgs.fileConvInputFile).toAbsolute.parent, inArgs.projectId, itemConfId).toList
 val len = itemConfs.map(_.length.toInt)
 val names = itemConfs.map(_.itemId)
 val domains = itemConfs.map(_.cnvType)
 def makeSliceLen(len: Seq[Int]) = len.foldLeft((0, List.empty[(Int, Int)])){(l, r) =>(l._1 + r, l._2 :+ (l._1, l._1 + r))}
 val (totalLen_, sliceLen) = makeSliceLen(len)
 val ziped = names.zip(domains)
 val (nameList, domainList) = ziped.filter{
      case (names, domain) => !(domain.startsWith(Converter.NOT_USE_PREFIX))
      }.unzip
 def cnvFromFixed(names: Seq[String], domains: Seq[String], sliceLen: List[(Int, Int)])(inData: Array[Byte]) = {
      val dataAndDomainsAndNames = sliceLen.map{
         case (start, end) => inData.slice(start, end)
         }.zip(domains).zip(names)
 val result = Converter.domainConvert(dataAndDomainsAndNames, charEnc)
Row.fromSeq(result)
      }
 val droppedDf = df.drop("ROW_ERR").drop("ROW_ERR_MESSAGE")
 val rdd = droppedDf.rdd.map{ orgRow => val row = cnvFromFixed(names, domains, sliceLen)(orgRow.getAs[Array[Byte]](binaryRecordName))
Row.merge(orgRow, row)
}
 val schema = Converter.makeSchema(nameList).foldLeft(droppedDf.schema){(l, r) =>l.add(r.name, r.dataType)}
SparkContexts.context.createDataFrame(rdd, schema).drop(binaryRecordName)
   }
}