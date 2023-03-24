package d2k.common

import scala.io.Source

object KanaConverter extends Serializable {
  private[this] val charEnc = "MS932"

  private[this] def readData(fileName: String) = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(s"kanaConv/$fileName"), charEnc).getLines

  private[this] val cnvOrg = " " :: readData("kanaConv.org").toList
  private[this] val cnvOrgSelect = " " :: readData("kanaConvSearch.org").toList
  private[this] val converted = " " :: readData("kanaConv.converted").toList
  private[this] val convertedSelect = " " :: readData("kanaConvSearch.converted").toList
  private[this] val zenkakuCnv = (inStr: String) => { if (inStr.getBytes(charEnc).size >= 2) { "*" } else { inStr } }
  private[this] val cnvMap = cnvOrg.zip(converted).toMap
  private[this] val cnvMapSelect = cnvOrgSelect.zip(convertedSelect).toMap

  def apply(inStr: String) = { if (inStr != null) { inStr.map(c => cnvMap.getOrElse(c.toString, zenkakuCnv(c.toString))).mkString } else { inStr } }
  def select(inStr: String) = { if (inStr != null) { inStr.map(c => cnvMapSelect.getOrElse(c.toString, zenkakuCnv(c.toString))).mkString } else { inStr } }
}
