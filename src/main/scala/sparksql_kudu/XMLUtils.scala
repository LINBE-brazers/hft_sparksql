package sparksql_kudu

import java.io.InputStream

import org.dom4j.{Document, Element}
import org.dom4j.io.SAXReader

import scala.collection.mutable

object XMLUtils {

  /**
   * 获取xml中内容
   *
   * @return
   */
  def getXmlStr(xmlPath: String): String = {
    val reader = new SAXReader()
    var document: Document = null
    reader.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
    reader.setFeature("http://xml.org/sax/features/external-general-entities", false)
    reader.setFeature("http://xml.org/sax/features/external-parameter-entities", false)
    val xml: InputStream = this.getClass.getResourceAsStream(xmlPath)
    document = reader.read(xml)
    val root = document.getRootElement
    root.getText
  }

  /**
   * 对sql中的占位符进行解析
   *
   * @param sql 传入的sql
   * @return
   */
  def selectMapping(sql: String, valueMap: mutable.HashMap[String, String]): String = {
    val iter = valueMap.iterator
    val stringBuffer: StringBuffer = new StringBuffer(sql)
    var replaceSql = ""
    var resultSql = ""

    while (iter.hasNext) {
      val entry: (String, String) = iter.next()
      val key: String = entry._1.trim
      val value: String = entry._2
      replaceSql = stringBuffer.toString.replace("${" + key + "}", value)
      stringBuffer.delete(0, stringBuffer.length())
      stringBuffer.append(replaceSql)
      resultSql = stringBuffer.toString
    }

    resultSql
  }
}
