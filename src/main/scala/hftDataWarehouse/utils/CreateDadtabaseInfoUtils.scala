package hftDataWarehouse.utils

import java.sql.DriverManager
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import utils.Config

import scala.collection.mutable.ArrayBuffer

/***
 * 连接关系型数据库相关的工具类
 */
object CreateDadtabaseInfoUtils {

  /**
   * 创建连接关系型数据库sqlserver的连接配置项
   * @return
   */
  def create_sqlserver_db_properties_info():Properties={
      val prop = new Properties()
      prop.put("username",Config.getProperty("user_name_sqlserver"))
      prop.put("password",Config.getProperty("passsword_sqlserver"))
      prop.put("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      prop.put("fetchsize","1000")
      prop
  }

  def create_sqlserver_1_db_properties_info():Properties={
      val prop = new Properties()
      prop.put("username",Config.getProperty("user_name_sqlserver_1"))
      prop.put("password",Config.getProperty("passsword_sqlserver_1"))
      prop.put("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      prop.put("fetchsize","1000")
      prop
  }




  /**
   * 创建连接关系型数据库mysql的连接配置项
   * @return
   */
  def create_mysql_db_properties_info():Properties={
      val prop = new Properties()
      prop.put("user",Config.getProperty("user_name_mysql"))
      prop.put("password",Config.getProperty("passsword_mysql"))
      prop.put("driver","com.mysql.jdbc.Driver")
      prop.put("fetchsize","1000")
      prop
  }


  /**
   * 创建SparkJdbcPredicates属性相关信息,请注意idNumber，
   * 在关系型数据库中构造自增序号时请注意命名
   * @param tableCntTotal
   * @param step
   * @return
   */
  def createSparkJdbcPredicatesInfo(tableCntTotal:Int,step:Int): Array[String] = {

      val result = tableCntTotal/step
      val arrayBuffer = ArrayBuffer[String]()
      var i = 1
      var temp = 0

      //构造数据同步时用到的分区条件，返回的是数组结构的数据
      while(i <= result + 1){
        val tempResult = i * step
        val str = " idNumber > " +temp+" and idNumber <= "+tempResult
        arrayBuffer += str
        temp = tempResult
        i = i + 1
      }

      //返回数组结构数据
      arrayBuffer.toArray

  }

  /**
   * 通过JDBC形式查找表的总数
   * @param tableName
   * @return
   */
  def findSqlServerTableCntInfo(dbsource_type:String,tableName:String): Int ={
    var tableCnt = 0
    try{
        //1.加载数据库驱动
        if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_MYSQL_TYPE)){
           Class.forName("com.mysql.jdbc.Driver")
        }else{
           Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
        }
        //2.得到链接
        var url = ""
        var username = ""
        var password = ""
        if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_SQL_SERVER_TYPE)){
            url = Config.getProperty("url_sqlserver")
            username = Config.getProperty("user_name_sqlserver")
            password = Config.getProperty("passsword_sqlserver")
        }else if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_SQL_SERVER_1_TYPE)){
            url = Config.getProperty("url_sqlserver_1")
            username = Config.getProperty("user_name_sqlserver_1")
            password = Config.getProperty("passsword_sqlserver_1")
        }else if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_MYSQL_TYPE)){
            url = Config.getProperty("url_mysql")
            username = Config.getProperty("user_name_mysql")
            password = Config.getProperty("passsword_mysql")
        }

        val connection = DriverManager.getConnection(url,username,password)
        val statement = connection.prepareStatement("select count(1) as cnt from "+tableName)
        val rs = statement.executeQuery()
        while(rs.next()) {
           tableCnt = rs.getInt("cnt")
        }
        if(null != rs){
           rs.close()
        }
        if(null != statement){
           statement.close()
        }
        if(null != connection){
           connection.close()
        }
    }catch{
      case ex: Exception => {
        println("查询表总数异常 Exception"+ex.printStackTrace())
      }
    }
    tableCnt
  }


  /**
   * 判断是否需要设置SparkJdbcPredicates，以及如果设置时返回数据分区;
   * 新增参数，用于判断是否需要去判断分区,有些表不需要分区
   * @param tableCnt
   * @return
   */
  def checkIsNeedSparkJdbcPredicates(tableCnt:Int,isNeedPartition:Boolean): Array[String] ={
        val buf: ArrayBuffer[String] = ArrayBuffer[String]()
        //需要分区情况下判断是否满足分区要求
        var isNeed = false
        var stepTemp = 0
        if(isNeedPartition){
          //低于50万的不需要分区，大于50万到300万的按50万分区
          if(tableCnt >= 500000 &&  tableCnt < 3000000){
            isNeed = true
            stepTemp = 500000
          }else if(tableCnt >= 3000000){//大于300万的按200万分区
            isNeed = true
            stepTemp = 2000000
          }
        }
        if(isNeed){
          createSparkJdbcPredicatesInfo(tableCnt,stepTemp)
        }else{
          buf.toArray
        }
  }

}
