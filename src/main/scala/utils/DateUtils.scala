package utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {

  val YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss"
  val YYYY_MM_DD = "yyyy-MM-dd"

  /**
   * 字符串转化为日期Date类型
   *
   * @param str
   * @param pattern
   * @return
   */
  def strToDate(str: String, pattern: String = YYYY_MM_DD_HH_MM_SS): Date = {
    val format = new SimpleDateFormat(pattern)
    format.parse(str)
  }


  /**
   * 日期Date转为String
   *
   * @param date
   * @param pattern
   * @return
   */
  def dateToString(date: Date, pattern: String): String = {
    val format = new SimpleDateFormat(pattern)
    format.format(date)
  }


  /**
   * 时间戳转为String
   *
   * @param time
   * @param pattern
   * @return
   */
  def stampToStr(time: Timestamp, pattern: String = YYYY_MM_DD_HH_MM_SS): String = {
    val format = new SimpleDateFormat(pattern)
    format.format(time)
  }

}
