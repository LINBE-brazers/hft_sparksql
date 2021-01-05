package hftDataWarehouse_new

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import hftDataWarehouse.utils.ConstantUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import utils.Config

import scala.collection.mutable.ArrayBuffer


object Relation_db_utils {


  /**
   * 根据关系型数据库表创建目标库的表
   * @param spark
   * @param kudu_Context
   * @param dbsource_type
   * @param targetType
   * @param hive_database_name
   * @param table_name
   * @param filterColumns
   */
  def common_create_target_table_info(spark:SparkSession,
                                      kudu_Context: KuduContext,
                                      dbsource_type:String,
                                      targetType:String,
                                      hive_database_name:String,
                                      table_name:String,
                                      filterColumns:String) ={
    try{
        var sql_string = ""
        //获取关系型数据库jdbc连接
        val connection: Connection = relation_db_connection_information(dbsource_type)
        //查询表结构，mysql、sqlserver查询方式不同
        if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_MYSQL_TYPE)){
           sql_string = "desc " + table_name
        }else{
           sql_string = "sp_columns  " + table_name
        }
        //创建statement
        val statement: PreparedStatement = connection.prepareStatement(sql_string)
        val rs: ResultSet = statement.executeQuery()
        //判断目标库的类型，执行不同的操作,目前是hive和kudu
        if(StringUtils.equals(ConstantUtils.TARGET_TYPE_HIVE,targetType)){
            create_bigdata_hive_table_by_relational_table(spark,rs,dbsource_type,hive_database_name,table_name,filterColumns)
        }else if(StringUtils.equals(ConstantUtils.TARGET_TYPE_KUDU,targetType)){
            //todo
        }
        //关闭打开的资源
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
         println("查询表结构异常 Exception"+ex.printStackTrace())
      }
    }
  }


  /***
   *
   * @param spark
   * @param rs
   * @param dbsource_type
   * @param hive_database_name
   * @param table_name
   * @param filterColumns
   */
  def create_bigdata_hive_table_by_relational_table(spark:SparkSession,
                                                     rs:ResultSet,
                                                     dbsource_type:String,
                                                     hive_database_name:String,
                                                     table_name:String,
                                                     filterColumns:String): Unit ={
    //db sql转换为hive sql
    val resultSql = sqlserver_columns_to_hive_columns(rs,filterColumns)
    //拼接创建表的sql
    val create_table_sql = "create table IF NOT EXISTS  "+hive_database_name+"."+table_name+"("+resultSql+") stored as PARQUET"
    spark.sql(create_table_sql)




  }


  /**
   * 构建创建hive表所需字段
   * @param rs
   * @param filterColumns
   * @return
   */
  def sqlserver_columns_to_hive_columns(rs: ResultSet,filterColumns:String): String ={
      var filterColumnsArray: Array[String] = Array[String]();
      if(filterColumns != null && (!filterColumns.equals(""))){
        filterColumnsArray = filterColumns.split(",")
      }
      val arrayBuffer = ArrayBuffer[String]()
      var temp_str = ""
      while(rs.next()) {
         val column_name = rs.getString("COLUMN_NAME")
         val type_name = rs.getString("TYPE_NAME")
         //过滤掉不需要同步的字段
         if(!filterColumnsArray.contains(column_name)){
             if(StringUtils.equals(ConstantUtils.TYPE_INT,type_name)){
               temp_str = ConstantUtils.TYPE_INT
             }else if(StringUtils.equals(ConstantUtils.TYPE_BIGINT,type_name)){
               temp_str = ConstantUtils.TYPE_BIGINT
             }else if(StringUtils.equals(ConstantUtils.TYPE_FLOAT,type_name)){
               temp_str = ConstantUtils.TYPE_FLOAT
             }else if(StringUtils.equals(ConstantUtils.TYPE_DOUBLE,type_name)){
               temp_str =  ConstantUtils.TYPE_DOUBLE
             }else{
               temp_str = ConstantUtils.TYPE_STRING
             }
             arrayBuffer += column_name+" "+temp_str
         }
      }
      arrayBuffer.mkString(",")
  }


  /**
   * 创建关系型数据库连接
   * @param dbsource_type
   * @return
   */
  def  relation_db_connection_information(dbsource_type:String): Connection ={
      var connection:Connection = null
      try{
          //1.针对不同的数据源，增加不同的连接信息
          var url = ""
          var username = ""
          var password = ""
          if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_SQL_SERVER_TYPE)){
              url = Config.getProperty("url_sqlserver")
              username = Config.getProperty("user_name_sqlserver")
              password = Config.getProperty("passsword_sqlserver")
              Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
          }else if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_SQL_SERVER_1_TYPE)){
              url = Config.getProperty("url_sqlserver_1")
              username = Config.getProperty("user_name_sqlserver_1")
              password = Config.getProperty("passsword_sqlserver_1")
              Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
          }else if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_MYSQL_TYPE)){
              url = Config.getProperty("url_mysql")
              username = Config.getProperty("user_name_mysql")
              password = Config.getProperty("passsword_mysql")
              Class.forName("com.mysql.jdbc.Driver")
          }
          //创建连接信息
          connection = DriverManager.getConnection(url,username,password)
      }catch{
        case ex: Exception => {
          println("查询表总数异常 Exception"+ex.printStackTrace())
        }
      }
      connection
  }

}
