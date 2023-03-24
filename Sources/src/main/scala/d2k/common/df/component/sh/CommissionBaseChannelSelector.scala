package d2k.common.df.component.sh

import d2k.common.df.template.sh._
import d2k.common.df.executor.Nothing

object CommissionBaseChannelSelector {
  private[CommissionBaseChannelSelector] trait trComm試算 extends CommissionBaseChannelSelectorTmpl {
    val info = CommissionBaseChannelSelectorInfo("01", "1", "1")
  }
  def comm試算 = new trComm試算 {}
  def comm試算(uniqueKeys: String*) = new trComm試算 { override val groupingKeys = uniqueKeys }

  private[CommissionBaseChannelSelector] trait trComm実績_月次手数料 extends CommissionBaseChannelSelectorTmpl {
    val info = CommissionBaseChannelSelectorInfo("02", "1", "1")
  }
  def comm実績_月次手数料 = new trComm実績_月次手数料 {}
  def comm実績_月次手数料(uniqueKeys: String*) = new trComm実績_月次手数料 { override val groupingKeys = uniqueKeys }

  private[CommissionBaseChannelSelector] trait trComm実績_割賦充当 extends CommissionBaseChannelSelectorTmpl {
    val info = CommissionBaseChannelSelectorInfo("03", "1", "1")
  }
  def comm実績_割賦充当 = new trComm実績_割賦充当 {}
  def comm実績_割賦充当(uniqueKeys: String*) = new trComm実績_割賦充当 { override val groupingKeys = uniqueKeys }

  private[CommissionBaseChannelSelector] trait trComm実績_直営店 extends CommissionBaseChannelSelectorTmpl {
    val info = CommissionBaseChannelSelectorInfo("04", "0", "0")
  }
  def comm実績_直営店 = new trComm実績_直営店 {}
  def comm実績_直営店(uniqueKeys: String*) = new trComm実績_直営店 { override val groupingKeys = uniqueKeys }

  private[CommissionBaseChannelSelector] trait trComm毎月割一時金 extends CommissionBaseChannelSelectorTmpl {
    val info = CommissionBaseChannelSelectorInfo("05", "1", "1")
  }
  def comm毎月割一時金 = new trComm毎月割一時金 {}
  def comm毎月割一時金(uniqueKeys: String*) = new trComm毎月割一時金 { override val groupingKeys = uniqueKeys }
}