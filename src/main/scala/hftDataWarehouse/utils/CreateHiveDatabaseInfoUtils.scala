package hftDataWarehouse.utils

import java.text.SimpleDateFormat
import java.util.Properties

import hftDataWarehouse.load.SparkTaskSchedulingInfo
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Config

/**
 * 与HiveDatabaseInfo相关的工具类
 */
object CreateHiveDatabaseInfoUtils {

  /**
   * 创建数据库
   * @param spark
   * @param databaseName
   */
  def create_database_info(spark:SparkSession,databaseName:String):Unit={
    spark.sql("create database IF NOT EXISTS "+databaseName)
    //创建调度任务相关执行表
    CreateHFTHiveTableInfo.create_table_spark_task_scheduling_info(spark,databaseName)
  }


  /**
   * 从关系型数据库同步数据至hive表
   */
  def load_relationDbTable_to_hiveTable(dbsource_type:String,task_name:String,spark: SparkSession,hivedatabase:String,real_tableName:String,customize_tableName:String,dataFrame_temp_view:String,isNeedPartition:Boolean): Unit ={

      //查询同步时表总数，并记录此时的时间戳
      val beging_time = System.currentTimeMillis()
      val tablecnt = CreateDadtabaseInfoUtils.findSqlServerTableCntInfo(dbsource_type,real_tableName)
      //判断是否需要分区，主要是根据数据量大小来判断
      val array: Array[String] = CreateDadtabaseInfoUtils.checkIsNeedSparkJdbcPredicates(tablecnt,isNeedPartition)
      var isNeed = false
      if( array != null && array.length > 0){
         isNeed = true
      }

      //根据传入值加载不同的数据源
      var url = ""
      var prop:Properties = null
      if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_SQL_SERVER_TYPE)){
          prop = CreateDadtabaseInfoUtils.create_sqlserver_db_properties_info()
          url = Config.getProperty("url_sqlserver")
      }else if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_SQL_SERVER_1_TYPE)){
          prop = CreateDadtabaseInfoUtils.create_sqlserver_1_db_properties_info()
          url = Config.getProperty("url_sqlserver_1")
      }else if(StringUtils.endsWithIgnoreCase(dbsource_type,ConstantUtils.DB_MYSQL_TYPE)){
          prop = CreateDadtabaseInfoUtils.create_mysql_db_properties_info()
          url = Config.getProperty("url_mysql")
      }

      //开始同步数据，并将数据保存至hiv表中
      insert_into_table(spark,url, customize_tableName,prop,dataFrame_temp_view, array,hivedatabase,isNeed,real_tableName)

      //同步结束后再一次执行查询关系型数据库的数据总量，便于误差对比
      val end_count = CreateDadtabaseInfoUtils.findSqlServerTableCntInfo(dbsource_type,real_tableName)

      //保存执行结果，便于后续问题排查
      val end_time = System.currentTimeMillis()
      val project_name = "hftDataWarehouse"
      insert_table_spark_task_scheduling_info(project_name,task_name,hivedatabase,beging_time,end_time,real_tableName,tablecnt,end_count,spark)
  }

  /**
   * 保存数据到对应的hive表中
   * @param spark
   * @param url
   * @param RDTableName
   * @param prop
   * @param tableNameView
   * @param predicates
   * @param hiveDatabase
   * @param hiveTableName
   */
  def insert_into_table(spark:SparkSession,url:String,
                        RDTableName:String,prop:Properties,
                        tableNameView:String,predicates:Array[String],
                        hiveDatabase:String,isNeedPredicates:Boolean ,
                        hiveTableName:String):Unit={

    //大表需要分区，加快数据同步
    var df: DataFrame = null
    if(isNeedPredicates){
       df = spark.read.jdbc(url,RDTableName,predicates,prop)
    }else{
       df = spark.read.jdbc(url,RDTableName,prop)
    }
    df.createOrReplaceGlobalTempView(tableNameView)
    val insert_sql = "insert into table "+hiveDatabase+".ods_hft_sqlserver_"+hiveTableName+" select * from global_temp."+tableNameView
    spark.sql(insert_sql)
    spark.sqlContext.dropTempTable("global_temp."+tableNameView)
  }

  /**
   * 保存调度任务执行情况
   * @param project_name
   * @param task_name
   * @param database_name
   * @param beging_time
   * @param end_time
   * @param table_name
   * @param begin_count
   * @param end_count
   * @param spark
   */
  def insert_table_spark_task_scheduling_info(project_name:String,
                                              task_name:String,database_name:String,
                                              beging_time:Long,end_time:Long,table_name:String,
                                              begin_count:Int,end_count:Int,spark:SparkSession): Unit ={

        val dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val create_time = dateformat.format(System.currentTimeMillis)
        val durationTime = ((end_time-beging_time)/1000)+""
        val beging_time_t = dateformat.format(beging_time)
        val end_time_t = dateformat.format(end_time)
        val Difference = end_count - begin_count
        val isSuccess = "Y"
    ////    val sql = "insert into "+database_name+".ods_spark_task_scheduling_info values('"+project_name+"','"+task_name+"','"+database_name+"','"+table_name+"','"+create_time+"','"+beging_time_t+"','"+end_time_t+"','"+durationTime+"','"+begin_count+"','"+end_count+"','"+Difference+"','"+isSuccess+"')"
    ////    spark.sql(sql)
        import spark.implicits._
        val df = Seq(
          SparkTaskSchedulingInfo(
            project_name,task_name,database_name,
          table_name,create_time,beging_time_t,
          end_time_t,durationTime,begin_count+"",
          end_count+"",Difference+"",isSuccess)
        ).toDF()
        df.write.insertInto(database_name+".ods_spark_task_scheduling_info")
  }

}
