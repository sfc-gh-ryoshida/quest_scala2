package spark.common

import SparkContexts._
import SparkContexts._
import scala.util.Try
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.jdbc.JdbcType
import java.sql.Types._
import org.apache.spark.sql.types._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.functions._
import java.sql.PreparedStatement
import java.sql.Date
import java.sql.Timestamp
import java.sql.Types
import org.apache.spark.sql.sources.Filter
import java.sql.Connection
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite

case class DbInfo(
                   url: String, user: String, password: String, charSet: String = "JA16SJISTILDE",
                   commitSize: Option[Int] = Try(sys.env(DbCtl.envName.COMMIT_SIZE)).map(_.toInt).toOption,
                   isDirectPathInsertMode: Boolean = {
                     Try(sys.env(DbCtl.envName.DPI_MODE)).map(DbCtl.checkTrueOrOn).getOrElse(false)
                   },
                   fetchSize: Option[Int] = Try(sys.env(DbCtl.envName.FETCH_SIZE)).map(_.toInt).toOption) {
  val baseMap = Map(
    JDBCOptions.JDBC_URL -> url,
    JDBCOptions.JDBC_TABLE_NAME -> "DUAL",
    "user" -> user,
    "password" -> password,
    "charSet" -> charSet,
    "driver" -> DbCtl.ORACLE_DRIVER)

  def toOptions = {
    new JDBCOptions(fetchSize.map(fs => baseMap + (JDBCOptions.JDBC_BATCH_FETCH_SIZE -> fs.toString)).getOrElse(baseMap))
  }

  def toOptions(tableName: String) = {
    val addFetchSize = fetchSize.map(fs => baseMap + (JDBCOptions.JDBC_BATCH_FETCH_SIZE -> fs.toString)).getOrElse(baseMap)
    new JDBCOptions(addFetchSize + (JDBCOptions.JDBC_TABLE_NAME -> tableName))
  }

  def toOptionsInWrite(tableName: String) = {
    val addFetchSize = fetchSize.map(fs => baseMap + (JDBCOptions.JDBC_BATCH_FETCH_SIZE -> fs.toString)).getOrElse(baseMap)
    new JdbcOptionsInWrite(addFetchSize + (JDBCOptions.JDBC_TABLE_NAME -> tableName))
  }
}

object DbCtl extends Serializable {
  object envName {
    val COMMIT_SIZE = "COMMIT_SIZE"
    val DPI_MODE = "DPI_MODE"
    val FETCH_SIZE = "FETCH_SIZE"
  }

  lazy val dbInfo1 = DbInfo(sys.env("DB_URL"), Try(sys.env("DB_USER")).getOrElse(""), Try(sys.env("DB_PASSWORD")).getOrElse(""))
  lazy val dbInfo2 = DbInfo(sys.env("DB_URL2"), Try(sys.env("DB_USER2")).getOrElse(""), Try(sys.env("DB_PASSWORD2")).getOrElse(""))

  val ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver"

  val checkTrueOrOn = (s: String) => {
    val d = s.toLowerCase
    if (d == "true" || d == "on") true else false
  }

  def makeWhere10(columnName: String) = (
    (0 to 9).map(cnt => f"substr(${columnName},-1,1) = '$cnt'") :+ s" substr(${columnName},-1,1) not in ('0','1','2','3','4','5','6','7','8','9') ").toArray

  def localImport(tableName: String, cnt: Int = 100) = {
    val orgTb = new DbCtl(DbCtl.dbInfo2).readTable(tableName)

    val posgre = new DbCtl()
    import posgre.implicits._
    context.createDataFrame(sc.makeRDD(orgTb.take(cnt)), orgTb.schema).writeTable(tableName, SaveMode.Overwrite)
    posgre.readTable(tableName).show
  }

  val readAllData = Array("1 = 1")

  type UpdateMode = Int
  val UPDATE: UpdateMode = 0
  val UPSERT: UpdateMode = 1
}

case object OracleDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle") || url.contains("oracle")

  override def getCatalystType(
                                sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.NUMERIC && size == 0) {
      Option(DecimalType(DecimalType.MAX_PRECISION, 10))
    } else {
      None
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("VARCHAR2(255)", VARCHAR))
    case LongType   => Some(JdbcType("NUMBER", INTEGER))
    case _          => None
  }
}

case object DerbyDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:derby") || url.contains("derby")
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("CLOB", CLOB))
    case _          => None
  }
}

class DbCtl(val dbInfo: DbInfo = DbCtl dbInfo1) extends Logging with Serializable {
  import java.util.Properties
  import org.apache.spark.sql._

  val props = new Properties
  props.put("user", dbInfo.user)
  props.put("password", dbInfo.password)
  props.put("charSet", dbInfo.charSet)
  props.put("driver", DbCtl.ORACLE_DRIVER)
  sys.props.get(DbCtl.envName.FETCH_SIZE).orElse(dbInfo.fetchSize)
    .foreach { fs => props.put("fetchsize", fs.toString) }

  JdbcDialects.registerDialect(OracleDialect)
  JdbcDialects.registerDialect(DerbyDialect)

  def getTableFullName(tableName: String) = {
    val sql = "SELECT TABLE_OWNER FROM ALL_SYNONYMS WHERE TABLE_NAME = ? AND (OWNER ='PUBLIC' OR OWNER = ?) ORDER BY DECODE(OWNER,'PUBLIC',1,0)"
    val rs = prepExecSql(tableName, sql) { prs =>
      prs.setString(1, tableName.toUpperCase())
      prs.setString(2, dbInfo.user.toUpperCase())
    }
    if (rs.next()) s"${rs.getString(1)}.${tableName}" else tableName
  }

  def clearTable(tableName: String) = {
    elapse("clearTable") {
      val targetTable = if (tableName.contains(".")) tableName else Try(getTableFullName(tableName)).getOrElse(tableName)
      Try(truncateTable(targetTable)).recover { case e => errorLog(s"FAILED TO TRUNCATE ${targetTable}", e); deleteTable(tableName) }.get
    }
  }

  def execSql(tableName: String, sql: String) = {
    println(tableName, sql)
    JdbcUtils.createConnectionFactory(dbInfo.toOptions(tableName))().prepareStatement(sql).executeUpdate
  }
  def prepExecSql(tableName: String, sql: String)(prsFunc: PreparedStatement => Unit) = {
    val prep = JdbcUtils.createConnectionFactory(dbInfo.toOptions(tableName))().prepareStatement(sql)
    prsFunc(prep)
    prep.executeQuery
  }
  def truncateTable(tableName: String) = execSql(tableName, s"TRUNCATE TABLE $tableName")
  def dropTable(tableName: String) =
    JdbcUtils.dropTable(JdbcUtils.createConnectionFactory(dbInfo.toOptions)(), tableName, dbInfo.toOptions)
  def deleteTable(tableName: String) = execSql(tableName, s"DELETE FROM $tableName")
  def columnTypes(conn: Connection, tableName: String) = {
    val result = conn.getMetaData.getColumns(null, null, tableName.toUpperCase, "%")
    new Iterator[(String, Int)] {
      def hasNext = result.next
      def next() = (result.getString("COLUMN_NAME"), result.getString("DATA_TYPE").toInt)
    }.toMap
  }

  def changeColToDfTypes(colType: Int, dfTypes: DataType) = dfTypes match {
    case DateType      => DATE
    case TimestampType => TIMESTAMP
    case IntegerType   => INTEGER
    case LongType      => DOUBLE
    case DecimalType() => DECIMAL
    case _             => colType
  }

  def readTable(tableName: String) = {
    context.read.jdbc(dbInfo.url, tableName, props)
  }

  def readTable(tableName: String, where: Array[String]) = {
    context.read.jdbc(dbInfo.url, tableName, where, props)
  }

  def readTable(tableName: String, requiredColumns: Array[String], where: Array[String]) = {
    JdbcCtl.readTable(this, tableName, requiredColumns, where)
  }

  def loanConnection(tableName: String, sqlStr: String, executeBatch: Boolean = true)(f: PreparedStatement => Unit) {
    val jdbcConn = JdbcUtils.createConnectionFactory(dbInfo.toOptions(tableName))()
    val prs = jdbcConn.prepareStatement(sqlStr)
    try {
      f(prs)
      if (executeBatch) prs.executeBatch
    } finally {
      Some(prs).foreach(_.close)
      Some(jdbcConn).foreach(_.close)
    }
  }

  val recordProcessor = (tableName: String, sqlStr: String, structType: org.apache.spark.sql.types.StructType, fieldNames: Array[String]) =>
    (iter: Iterator[org.apache.spark.sql.Row]) => {
      loanConnection(tableName, sqlStr, false) { prs =>
        lazy val colType = columnTypes(prs.getConnection, tableName)
        val commitSize = sys.props.get(DbCtl.envName.COMMIT_SIZE).map(_.toInt).orElse(dbInfo.commitSize)
        commitSize.map { cs =>
          iter.sliding(cs.toInt, cs.toInt).foreach { commitBlock =>
            commitBlock.foreach(setValues(colType, structType, fieldNames, prs))
            prs.executeBatch()
          }
        }.getOrElse {
          iter.foreach { row =>
            setValues(colType, structType, fieldNames, prs)(row)
          }
          prs.executeBatch()
        }
      }
    }

  def setValues(columnTypes: Map[String, Int], structType: StructType, cols: Array[String], prs: PreparedStatement)(row: Row) = {
    def setPreparedValue(preps: PreparedStatement, col: String, idx: Int) = {
      structType(col).dataType match {
        case _ if row.isNullAt(row.fieldIndex(col)) => preps.setNull(idx + 1, changeColToDfTypes(columnTypes(col), structType(col).dataType))
        case StringType                             => preps.setString(idx + 1, row.getAs[String](col))
        case DateType                               => preps.setDate(idx + 1, row.getAs[Date](col))
        case TimestampType                          => preps.setTimestamp(idx + 1, row.getAs[Timestamp](col))
        case IntegerType                            => preps.setInt(idx + 1, row.getAs[Int](col))
        case LongType                               => preps.setLong(idx + 1, row.getAs[Long](col))
        case DecimalType()                          => preps.setBigDecimal(idx + 1, row.getAs[java.math.BigDecimal](col))
      }
      preps
    }
    cols.zipWithIndex.foldLeft(prs) { case (preps, (col, idx)) => setPreparedValue(preps, col, idx) }
    prs.addBatch
  }

  def insertAccelerated(df: DataFrame, tableName: String, mode: SaveMode = SaveMode.Append, hint: String = "") = {
    val structType = df.schema
    val fieldNames = structType.fieldNames
    val columnsStr = fieldNames.mkString(",")
    val bindStr = fieldNames.map(_ => "?").mkString(",")
    def nonDirectInsert = {
      val insertStr = s"""
      insert /*+ ${hint} */ into ${tableName}(${columnsStr}) values(${bindStr})
      """

      if (mode == SaveMode.Overwrite) clearTable(tableName)
      df.foreachPartition(recordProcessor(tableName, insertStr, structType, fieldNames))
    }

    def directInsert = {
      val insertStr = s"""
      insert /*+ APPEND_VALUES ${hint} */ into ${tableName}(${columnsStr}) values(${bindStr})
      """

      if (mode == SaveMode.Overwrite) clearTable(tableName)
      df.coalesce(1).foreachPartition(recordProcessor(tableName, insertStr, structType, fieldNames))
    }

    val dpi = sys.props.get(DbCtl.envName.DPI_MODE)
      .map(DbCtl.checkTrueOrOn)
      .getOrElse(dbInfo.isDirectPathInsertMode)
    if (dpi) directInsert else nonDirectInsert
  }

  def insertNotExists(df: DataFrame, tableName: String, inKeys: Seq[String], mode: SaveMode = SaveMode.Append, hint: String = "") = {
    val keysStr = inKeys.mkString(",")
    val structType = df.schema
    val fieldNames = structType.fieldNames
    val columnsStr = fieldNames.mkString(",")
    val bindStr = fieldNames.map(_ => "?").mkString(",")
    val insertStr = s"""
      insert /*+ ignore_row_on_dupkey_index(${tableName}(${keysStr})) ${hint} */ into ${tableName}(${columnsStr}) values(${bindStr})
      """
    if (mode == SaveMode.Overwrite) clearTable(tableName)
    df.foreachPartition(recordProcessor(tableName, insertStr, structType, fieldNames))
  }

  def deleteRecords(df: DataFrame, tableName: String, inKeys: Set[String], hint: String = "") = {
    val keysArray = inKeys.toArray
    val keysStr = keysArray.map(k => s"${k.toLowerCase} = ?").mkString(" and ")
    val deleteStr = s"""
      delete /*+ ${hint} */ from $tableName where $keysStr
      """

    val structType = df.schema
    df.foreachPartition(recordProcessor(tableName, deleteStr, structType, keysArray))
  }

  def updateRecords = updateRecordsBase(DbCtl.UPDATE) _
  def upsertRecords = updateRecordsBase(DbCtl.UPSERT) _

  def dropArrayData(target: Seq[String], dropList: Seq[String]) = dropList.foldLeft(target) { (l, r) =>
    l.filterNot(_ == r)
  }

  private[this] def updateRecordsBase(mode: DbCtl.UpdateMode)(df: DataFrame, tableName: String, inKeys: Set[String], ignoreColumnsForUpdate: Set[String] = Set.empty[String], hint: String = "") = {
    val keysArray = inKeys.toArray
    val cols = df.columns
    val colsWithoutKey = dropArrayData(df.columns, keysArray)
    val colsWithoutKeyAndIgnore = dropArrayData(colsWithoutKey, ignoreColumnsForUpdate.toSeq)
    val colsWithoutKeyStr = colsWithoutKeyAndIgnore.map(k => s"${k.toLowerCase} = ?").mkString(",")
    val keysStr = keysArray.map(k => s"${k.toLowerCase} = ?").mkString(" and ")
    val updateStr = s"""
      update /*+ ${hint} */ ${tableName}  set $colsWithoutKeyStr where $keysStr
      """

    val usingStr = cols.map(x => s"? $x").mkString(",")
    val onStr = keysArray.map(x => s"a.${x} = b.${x}").mkString(" and ")
    val updateSetStr = colsWithoutKeyAndIgnore.map(x => s"a.${x} = b.${x}").mkString(",")
    val insertStrLeft = cols.map(x => s"a.${x}").mkString(",")
    val insertStrRight = cols.map(x => s"b.${x}").mkString(",")
    val mergeStr = s"""
      merge /*+ ${hint} */ into ${tableName} a
        using (select ${usingStr} from dual) b
        on (${onStr})
      when matched then
        update set ${updateSetStr}
      when not matched then
        insert (${insertStrLeft}) values (${insertStrRight})
      """
    val structType = df.schema
    mode match {
      case DbCtl.UPSERT =>
        df.foreachPartition(recordProcessor(tableName, mergeStr, structType, cols))

      case DbCtl.UPDATE =>
        df.foreachPartition(recordProcessor(tableName, updateStr, structType, colsWithoutKeyAndIgnore ++: keysArray))
    }
  }

  object implicits {
    implicit class DbCtlDataFrame(df: DataFrame) {
      def writeTable(tableName: String, mode: SaveMode = SaveMode.Append) = mode match {
        case SaveMode.Overwrite => {
          clearTable(tableName)
          JdbcUtils.saveTable(df, None, true, dbInfo.toOptionsInWrite(tableName))
        }
        case SaveMode.Append => {
          JdbcUtils.saveTable(df, None, true, dbInfo.toOptionsInWrite(tableName))
        }
        case _ => df.write.mode(mode).jdbc(dbInfo.url, tableName, props)
      }

      /**
       * test or debug use only
       */
      def writeTableStandard(tableName: String, mode: SaveMode = SaveMode.Append) = mode match {
        case SaveMode.Overwrite => {
          clearTable(tableName)
          df.write.mode(mode).jdbc(dbInfo.url, tableName, props)
        }
        case _ => df.write.mode(mode).jdbc(dbInfo.url, tableName, props)
      }

      /**
       * test or debug use only
       */
      def autoCreateTable(tableName: String) {
        Try(df.limit(1).write.mode(SaveMode.Overwrite).jdbc(dbInfo.url, tableName, props))
        clearTable(tableName)
      }
    }
  }

  import implicits._
  def readParquetAndWriteDb(readParquetBasePath: String, readParquetPath: String, writeTableName: String, saveMode: SaveMode = SaveMode.Append)(proc: DataFrame => DataFrame = df => df) = {
    proc(new PqCtl(readParquetBasePath).readParquet(readParquetPath)).writeTable(writeTableName, saveMode)
  }
}
