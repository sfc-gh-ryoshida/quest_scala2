package d2k.appdefdoc.parser

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter

class ComponentFlowParserTest extends WordSpec with MustMatchers with BeforeAndAfter {
  "flow" should {
    "normal end" in {
      val testdata1 = """
"01_PqToDf\nFK向け顧客契約(中間)_PQ取得抽出" as 01_PqToDf --> "03_DfJoinToDf\nFK向け顧客契約ファイル料金プラン取付" as 03_DfJoinToDf

        """
      val testdata = """
"01_PqToDf\nFK向け顧客契約(中間)_PQ取得抽出" as 01_PqToDf --> "03_DfJoinToDf\nFK向け顧客契約ファイル料金プラン取付" as 03_DfJoinToDf
"02_PqToDf\nポスペ加入者台帳用料金系サービス情報PQ取得" as 02_PqToDf --> 03_DfJoinToDf
03_DfJoinToDf --> "05_DfJoinToDf\nFK向け顧客契約ファイルデータプリペ区分取付" as 05_DfJoinToDf
"04_DbToDf\nＬＴＥプリペイド料金プランテーブル取得" as 04_DbToDf --> 05_DfJoinToDf
05_DfJoinToDf --> "07_DfJoinToDf\nFK向け顧客契約ファイルデータ抽出" as 07_DfJoinToDf
"06_FileToDf\nmtGerberaFeePlnPrm_Gerbera料金プランパラメータファイル取得" as 06_FileToDf --> 07_DfJoinToDf
07_DfJoinToDf --> "08_DfToPq\nFK向け顧客契約Pqファイル出力" as 08_DfToPq
08_DfToPq --> (*)

        """
      ComponentFlowParser(testdata)
    }
  }
}