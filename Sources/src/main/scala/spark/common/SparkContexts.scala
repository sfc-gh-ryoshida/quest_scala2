package spark.common

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SparkSession

object SparkContexts {
  val conf = new SparkConf()
  val session = SparkSession.builder.enableHiveSupport.config(conf).getOrCreate
  val sc = session.sparkContext
  val processTime = new java.sql.Timestamp(sc.startTime)
  val context = session.sqlContext
}
