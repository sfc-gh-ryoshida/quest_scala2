package d2k.common

import org.joda.time.format.DateTimeFormat
import java.sql.Date
import scala.io.Source
import scala.util.Try
import spark.common.DbInfo
import d2k.common.df.DbConnectionInfo
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import java.sql.Timestamp
import java.util.Properties
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

case class InputArgs(d2kBasePath: String, productionMode: String,
                     confPath: String, dataPath: String,
                     projectId: String, processId: String, applicationId: String,
                     runningDateFileFullPath: String,
                     confBasePath:            String,

                     fileConvInputFile:    String,
                     fileConvOutputFile:   String,
                     baseInputFilePath:    String,
                     baseOutputFilePath:   String,
                     baseErrProofFilePath: String,
                     inputRunningDates:    Array[String],
                     inRunningSqlDate:     Date,
                     isDebug:              Boolean       = false) extends {
  val runningDates = inputRunningDates
  val runningSQLDate = inRunningSqlDate
} with ResourceDates with ResourceEnvs {
  val tableNameMapper: Map[String, String] = {
    Try {
      val mapperPath = sys.env("TABLE_MAPPER_FILE_PATH")
      if (mapperPath.isEmpty) {
        Map.empty[String, String]
      } else {
        Source.fromFile(mapperPath).getLines.map { line =>
          val splited = line.split("\t")
          (splited(0), splited(1))
        }.toMap
      }
    }.recover { case t => Map.empty[String, String] }.get
  }

  case class LastUpdateTime(from: Timestamp, to: Timestamp)
  def lastUpdateTime(tableName: String, readDbInfo: DbInfo = DbConnectionInfo.bat1) = {
    val options = new JDBCOptions(Map(
      JDBCOptions.JDBC_URL -> readDbInfo.url,
      JDBCOptions.JDBC_TABLE_NAME -> tableName,
      "user" -> readDbInfo.user,
      "password" -> readDbInfo.password,
      "charSet" -> readDbInfo.charSet))

    val ps = JdbcUtils.createConnectionFactory(options)()
      .prepareStatement("select DT_FROMUPDYMDTM, DT_TOUPDYMDTM from MOP012 where ID_TBLID = ?")
    ps.setString(1, tableName)
    val rs = ps.executeQuery

    val result = try {
      Iterator.continually((rs.next, rs)).takeWhile(_._1).map {
        case (_, rec) => LastUpdateTime(rec.getTimestamp("DT_FROMUPDYMDTM"), rec.getTimestamp("DT_TOUPDYMDTM"))
      }.toSeq
    } finally {
      rs.close
    }

    result.headOption.getOrElse(throw new IllegalArgumentException(s"tableName is not defined[$tableName]"))
  }
}

object InputArgs {
  def apply(d2kBasePath: String, productionMode: String, confPath: String, dataPath: String,
            projectId: String, processId: String, applicationId: String,
            runningDateFileFullPath: String) = {
    val confBasePath = s"$d2kBasePath/$productionMode/$confPath"
    val fileConvInputFile = s"$confBasePath/import/${projectId}_app.conf"
    val fileConvOutputFile = s"$confBasePath/export/${projectId}_app.conf"
    val baseInputFilePath = s"$d2kBasePath/$productionMode/$dataPath/output"
    val baseOutputFilePath = s"$d2kBasePath/$productionMode/$dataPath/output"
    val baseErrProofFilePath = s"$d2kBasePath/$productionMode/$dataPath/error"

    val runningDates = Source.fromFile(runningDateFileFullPath).getLines.toList(1).split(" ")
    val dateFormat = DateTimeFormat.forPattern("yyyyMMdd")
    val runningSQLDate = new Date(dateFormat.withZoneUTC.parseDateTime(runningDates(0)).getMillis)

    new InputArgs(d2kBasePath, productionMode, confPath, dataPath,
      projectId, processId, applicationId,
      runningDateFileFullPath, confBasePath, fileConvInputFile, fileConvOutputFile,
      baseInputFilePath, baseOutputFilePath, baseErrProofFilePath,
      runningDates, runningSQLDate)
  }

  def apply(projectId: String, processId: String, applicationId: String, runningDateFileFullPath: String): InputArgs = {
    apply("/D2Khome", "HN", "APL/conf/spark", "sparkWK/Parquet", projectId, processId, applicationId, runningDateFileFullPath)
  }
}
