package spark.common


/*EWI: SPRKSCL1142 => org.apache.spark.SparkConf is not supported*/
import org.apache.spark.SparkConf

/*EWI: SPRKSCL1142 => org.apache.spark.SparkContext is not supported*/
import org.apache.spark.SparkContext

/*EWI: SPRKSCL1142 => org.apache.spark.sql.hive.HiveContext is not supported*/
import org.apache.spark.sql.hive.HiveContext
import com.snowflake.snowpark.Session
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object SparkContexts {
   val conf = new SparkConf ()

   val session = SparkSession.builder.enableHiveSupport.config(conf).getOrCreate

   val sc = session.sparkContext

   val processTime = new java.sql.Timestamp (sc.startTime)

   val context = session.sqlContext
}