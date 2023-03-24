package org.apache.spark.sql

import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import spark.common.SparkContexts
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import spark.common.DbCtl
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.sources.Filter
import java.sql.Timestamp
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.Partition

object JdbcCtl {
  def readTable(dbCtl: DbCtl, tableName: String, requiredColumns: Array[String],
                where: Array[String] = Array("1 = 1"), whereFilter: Array[Filter] = Array.empty[Filter]) = {
    val partitions: Array[Partition] = where.zipWithIndex.map {
      case (w, idx) => JDBCPartition(w, idx)
    }
    val structTypes = JDBCRDD.resolveTable(dbCtl.dbInfo.toOptions(tableName))
    val reqStructTypes = pruneSchema(structTypes, requiredColumns)

    val rdd = JDBCRDD.scanTable(
      SparkContexts.sc, structTypes, requiredColumns, whereFilter, partitions, dbCtl.dbInfo.toOptions(tableName)).map { r =>
        val values = internalRowToRow(r, reqStructTypes).zipWithIndex.map {
          case (strType, idx) =>
            strType match {
              case _ if r.isNullAt(idx) == true => null
              case StringType                   => r.getString(idx)
              case TimestampType                => new Timestamp(r.getLong(idx) / 1000)
              case x: DecimalType               => r.getDecimal(idx, x.precision, x.scale).toBigDecimal
            }
        }
        Row(values: _*)
      }
    SparkContexts.context.createDataFrame(rdd, reqStructTypes)
  }

  def pruneSchema(schema: StructType, columns: Array[String]) = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(c => fieldMap(c)))
  }

  def internalRowToRow(iRow: InternalRow, schema: StructType) = {
    schema.map(_.dataType)
  }
}
