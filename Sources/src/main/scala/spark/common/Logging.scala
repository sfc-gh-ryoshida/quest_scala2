package spark.common

import org.slf4j.LoggerFactory

trait Logging extends Serializable {
  val logger = LoggerFactory.getLogger(this.getClass)

  def errorLog(message: String, t: Throwable) = logger.error(message, t)

  def isDebugEnabled = logger.isDebugEnabled

  def elapse(message: String)(func: => Unit) = {
    logger.info(s" Start[${message}]")
    val startTime = System.currentTimeMillis
    func
    val endTime = System.currentTimeMillis
    val elapse = BigDecimal(endTime - startTime) / 1000
    logger.info(f"finish[${message}] elapse:${elapse}%,.3fs")
  }
}