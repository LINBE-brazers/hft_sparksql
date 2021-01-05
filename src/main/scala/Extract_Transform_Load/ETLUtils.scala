package Extract_Transform_Load

import java.io.{BufferedReader, InputStreamReader}

import Extract_Transform_Load.multithread.ParameterDTONew
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object ETLUtils {

  /**
   * 对sql中的点位符进行解析
   *
   * @param sql
   * @param parameter
   * @return
   */
  def selectMapping(sql: String, parameter: ParameterDTONew): String = {
    val stringBuffer = new StringBuffer(sql)
    var replaceSql = ""
    var resultSql = ""
    val clazz = parameter.getClass
    val fields = clazz.getDeclaredFields

    fields.foreach(field => {
      val key = field.getName
      var value = clazz.getMethod("get" + getMethodName(key)).invoke(parameter).asInstanceOf[String]
      if (value == null) value = ""
      replaceSql = stringBuffer.toString.replace("${" + key + "}", value)
      stringBuffer.delete(0, stringBuffer.length())
      stringBuffer.append(replaceSql)
      resultSql = stringBuffer.toString
    })
    resultSql
  }


  /**
   * 将fieldName首字母转为大写
   *
   * @param fieldName
   * @return
   */
  def getMethodName(fieldName: String): String = {
    val items = fieldName.getBytes()
    items(0) = (items(0).asInstanceOf[Char] - 'a' + 'A').asInstanceOf[Byte]
    new String(items)
  }

  /**
   * 从HDFS中加载文件
   *
   * @param path
   * @return
   */
  def readFileFromHDFS(path: String): String = {
    val buffer = new StringBuffer()
    val conf = new Configuration()
    conf.set("fs.default.name", "hdfs://HDBDC")
    val fs = FileSystem.get(conf)
    val remotePath = new Path(path)
    val in = fs.open(remotePath)
    val reader = new BufferedReader(new InputStreamReader(in, "UTF-8"))
    var tempStr = ""
    while ( {
      tempStr = reader.readLine()
      tempStr != null
    }) {
      buffer.append(tempStr)
    }
    reader.close()
    buffer.toString
  }

  

}
