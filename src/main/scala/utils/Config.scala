package utils

import memberManager.PropertiesScalaUtils
import org.slf4j.LoggerFactory




/**
  * Created by 0619653X0 on 2019-6-4.
  */
object Config {
    val logger = LoggerFactory.getLogger(this.getClass)

    val properties = PropertiesScalaUtils.loadProperties("envs.properties")

    /**
      * 获取 env.properties配置文件中 key对应的值
      *
      * @param key
      * @return
      */

    def getProperty(key: String): String = {
        try{
            properties.getProperty(key)
        }catch {
            case e:Exception=>{
                logger.error("load property error:",e)
            }
                null
        }
    }

}


