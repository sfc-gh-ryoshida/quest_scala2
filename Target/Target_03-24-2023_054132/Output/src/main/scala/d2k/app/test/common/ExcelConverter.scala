package d2k.app.test.common

import spark.common.FileCtl
import java.io.File
import org.apache.poi.ss.usermodel.WorkbookFactory
import org.apache.poi.ss.usermodel.Cell
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object ExcelConverter {
   def toMarkdown(excelPath: String, sheet: String, mdFile: String) = {
   val data = to2DArray(excelPath, sheet)
 val colSize = data(0).filter(!_.isEmpty).size
 val x: Seq[List[String]] = Seq(
data(0).toList, 
data(0).map(_ =>"----").toList, 
data(0).toList, 
data(1).map{
      case xif x.endsWith("_ZD") => "ZD"
      case xif x.endsWith("_PD") => "PD"
      case "全角文字列" => "全角文字列"
      case _ => "半角文字列"
      }.toList.take(colSize), 
data(2).toList.take(colSize))
 val x2 = x ++ data.toList.drop(3).map(_.toList)
 val x3 = x2.map( a =>a.take(colSize).mkString("|", "|", "|"))
System.setProperty("line.separator", "\n")
FileCtl.writeToFile(mdFile, charEnc = "UTF-8"){ pw =>pw.println(s"# ${ sheet }")
x3.foreach(pw.println)
}
   }

   def to2DArray(path: String, sheetName: String) = {
   val sheet = getTargetSheet(path, sheetName)
 val rowCnt = sheet.getLastRowNum() + 1
(0 to rowCnt).flatMap{ i =>Option(sheet.getRow(i)).map{ row =>(0 to row.getLastCellNum).flatMap{ cellCnt =>Option(row.getCell(cellCnt)).map( cell =>getStrVal(cell))
}
}
}
   }

   def getStrVal(cell: Cell) = {
   cell.getCellType match {
         case Cell.CELL_TYPE_STRING => cell.getStringCellValue
         case Cell.CELL_TYPE_NUMERIC => cell.getNumericCellValue.toString
         case Cell.CELL_TYPE_FORMULA => try
            {cell.getStringCellValue}
         catch {
            case ex:IllegalStateException => cell.getNumericCellValue.toString
         }
         case Cell.CELL_TYPE_BLANK => ""
         case Cell.CELL_TYPE_BOOLEAN => cell.getBooleanCellValue.toString
         case Cell.CELL_TYPE_ERROR => cell.getStringCellValue
      }
   }

   def getTargetSheet(path: String, sheetName: String) = {
   val input = new File (path)
 val book = WorkbookFactory.create(input)
book.getSheet(sheetName)
   }
}