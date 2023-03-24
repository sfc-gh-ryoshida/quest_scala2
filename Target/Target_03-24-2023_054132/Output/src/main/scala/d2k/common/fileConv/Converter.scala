package d2k.common.fileConv

import com.snowflake.snowpark.types.StructType

/*EWI: SPRKSCL1142 => org.apache.spark.sql.types.StructField is not supported*/
import org.apache.spark.sql.types.StructField
import com.snowflake.snowpark.types.StringType
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class ErrMessage (name: String, domain: String, value: String, message: String) {
   override def toString = s"${ name },${ domain },[${ value }],${ message }"
}
 object Converter extends Serializable {
val REC_DIV_PREFIX = "レコード区分_"
 val NOT_USE_PREFIX = "未使用"
 val REC_DIV_EXTRACT = "D"
 object SYSTEM_COLUMN_NAME {
      val RECORD_INDEX = "_recordIndex_"

      val RECORD_LENGTH_ERROR = "_recordLengthError_"

      val ROW_ERROR = "ROW_ERR"

      val ROW_ERROR_MESSAGE = "ROW_ERR_MESSAGE"
   }
 def makeSchema(inNames: Seq[String]) = {
   val names = inNames :+ SYSTEM_COLUMN_NAME.ROW_ERROR :+ SYSTEM_COLUMN_NAME.ROW_ERROR_MESSAGE
StructType(names.map{
      case name => StructField(name, StringType, true)
      })
   }
 def makeSchemaWithRecordError(inNames: Seq[String]) = {
   val names = inNames :+ SYSTEM_COLUMN_NAME.ROW_ERROR :+ SYSTEM_COLUMN_NAME.ROW_ERROR_MESSAGE :+ SYSTEM_COLUMN_NAME.RECORD_LENGTH_ERROR
StructType(names.map{
      case name => StructField(name, StringType, true)
      })
   }
 def domainConvert(dataAndDomainsAndNames: Seq[((String, String), String)]) = {
   val (rowData, errMessage) = dataAndDomainsAndNames.foldLeft((Seq.empty[String], Seq.empty[ErrMessage])){(l, r) => val (convCols, errMessages) = l
 val ((data, domain), name) = r
if (domain == NOT_USE_PREFIX || domain.startsWith(REC_DIV_PREFIX))
         {
         l
         }
else
         {
         val target = if (data == null)
               {
               ""
               }
else
               {
               data
               }
DomainProcessor.exec(domain, target) match {
               case Right(d) => (convCols :+ d, errMessages)
               case Left(m) => (convCols :+ null, errMessages :+ ErrMessage(name, domain, data, m))
            }
         }
}
makeErrorMessage(errMessage, rowData)
   }
 def domainConvert(dataAndDomainsAndNames: Seq[((Array[Byte], String), String)], charEnc: String) = {
   val (rowData, errMessage) = dataAndDomainsAndNames.foldLeft((Seq.empty[String], Seq.empty[ErrMessage])){(l, r) => val (convCols, errMessages) = l
 val ((data, domain), name) = r
if (domain == NOT_USE_PREFIX)
         {
         l
         }
else
         {
         DomainProcessor.execArrayByte(domain, data, charEnc) match {
               case Right(d) => (convCols :+ d, errMessages)
               case Left(m) => (convCols :+ null, errMessages :+ ErrMessage(
name, domain, new String (data, if (charEnc == "JEF")
                  "ISO-8859-1"
else
                  charEnc), m))
            }
         }
}
makeErrorMessage(errMessage, rowData)
   }
 def makeErrorMessage(errMessage: Seq[ErrMessage], rowData: Seq[String]) = if (errMessage.isEmpty)
      {
      rowData :+ "false" :+ ""
      }
else
      {
      rowData :+ "true" :+ errMessage.mkString("|")
      }
 def removeHeaderAndFooter [A](data: Seq[A], hasHeader: Boolean, hasFooter: Boolean) = ((hasHeader, hasFooter) match {
      case (true, true) => data.drop(1).dropRight(1)
      case (true, false) => data.drop(1)
      case (false, true) => data.dropRight(1)
      case _ => data
   })
 def removeHeaderAndFooter(df: DataFrame, hasHeader: Boolean, hasFooter: Boolean, colNames: Seq[String], domainNames: Seq[String]) = {
   if (hasHeader || hasFooter)
         {
         val dataDivIdx = domainNames.indexWhere{ elem =>elem.startsWith(REC_DIV_PREFIX)}
 val dataDivColName = colNames(dataDivIdx)
df.filter(col(dataDivColName) === REC_DIV_EXTRACT).drop(dataDivColName)
         }
else
         {
         df
         }
   }
}