package memberManager

import java.io.InputStream
import java.util.Properties

import org.apache.log4j.Logger

/**
  * @title: PropertiesScalaUtils
  * @projectName youzu_spark_sql
  * @description: TODO
  * @author 122921390
  * @date 2019-11-2015:45
  */
object PropertiesScalaUtils {

  val logger = Logger.getLogger(this.getClass)

  def loadProperties(filePath: String): Properties = {
    val properties = new Properties()
    try {
      //文件要放到resource文件夹下
      val in: InputStream = PropertiesScalaUtils.getClass.getClassLoader.getResourceAsStream(filePath)
      properties.load(in)
      properties
    } catch {
      case e: Exception => {
        logger.error("get the config error:", e)
      }
        null
    }
  }
}
