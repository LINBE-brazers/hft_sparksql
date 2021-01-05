package Extract_Transform_Load

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
   * 创建连接关系型数据库的连接配置项
   * @return
   */
  def create_relation_db_properties_info(dbsource_type:String,db_ip:String,db_port:String,db_name:String,user_name:String,user_password:String):Properties={
      val prop = new Properties()
      prop.put("fetchsize","10000")
      if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_MYSQL_TYPE)){
        prop.put("user",user_name)
        prop.put("password",user_password)
        prop.put("driver","com.mysql.jdbc.Driver")
        val url = "jdbc:mysql://"+db_ip+":"+db_port+"/"+db_name+"?characterEncoding=utf8"
        prop.put("url",url)
      }else if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_SQL_SERVER_TYPE)){
        prop.put("username",user_name)
        prop.put("password",user_password)
        prop.put("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
        val url = "jdbc:sqlserver://"+db_ip+":"+db_port+";DatabaseName="+db_name
        prop.put("url",url)
      }
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
  def findRelationTableCntInfo(dbsource_type:String,tableName:String,prop:Properties): Int ={
    var tableCnt = 0
    try{
        Class.forName(prop.getProperty("driver"))
        //2.得到链接
        val url = prop.getProperty("url")
        var username:String = ""
        var password = prop.getProperty("password")
        if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_MYSQL_TYPE)){
            username = prop.getProperty("user")
        }else if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_SQL_SERVER_TYPE)){
            username = prop.getProperty("username")
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
  def checkIsNeedSparkJdbcPredicates(tableCnt:Int,isNeedPartition:String): Array[String] ={
        val buf: ArrayBuffer[String] = ArrayBuffer[String]()
        //需要分区情况下判断是否满足分区要求
        var isNeed = false
        var stepTemp = 0
        if(StringUtils.equals("Y",isNeedPartition)){
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
