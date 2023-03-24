package d2k.common

import org.slf4j.LoggerFactory
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
trait Logging extends Serializable {
   val logger = LoggerFactory.getLogger(this.getClass)

   def platformError(t: Throwable) = logger.error("[PLATFORM]", t)

   def appError(t: Throwable) = logger.error("[APP]", t)

   def isDebugEnabled = logger.isDebugEnabled

   def elapse(message: String)(func: =>Unit) = {
   logger.info(s" Start[${ message }]")
 val startTime = System.currentTimeMillis
func
 val endTime = System.currentTimeMillis
 val elapse = BigDecimal(endTime - startTime) / 1000
logger.info(f"finish[${ message }] elapse:${ elapse }%,.3fs")
   }
}