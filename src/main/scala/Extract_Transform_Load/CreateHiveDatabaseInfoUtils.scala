package Extract_Transform_Load

import java.text.SimpleDateFormat
import java.util.Properties
import hftDataWarehouse.load.SparkTaskSchedulingInfo
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    //创建hive数据库
    val sql = "create database IF NOT EXISTS "+databaseName
    spark.sql(sql)
    //创建调度任务相关执行表
    create_table_spark_task_scheduling_info(spark,databaseName)
  }


  /**
   * 创建hive表，公用方法
   * @param spark
   * @param databaseName
   * @param tableName
   * @param sql_hive
   * @param isDropTable
   */
  def create_table_common_center(spark:SparkSession,databaseName:String,tableName:String,sql_hive:String,isDropTable:String): Unit ={

    val hive_table_name = databaseName+"."+tableName
    //判断是否删除原表结构信息
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      val drop_sql = "drop table if exists  "+hive_table_name
      spark.sql(drop_sql)
    }

    //创建hive表
    val create_sql = "create table IF NOT EXISTS "+hive_table_name+"("+sql_hive+") stored as PARQUET"
    spark.sql(create_sql)

    //因为全量同步,需要清表
    val truncate_sql = "truncate table  "+hive_table_name
    spark.sql(truncate_sql)
  }


  /**
   * 创建hive数据库的spark_task_scheduling_info表,用于记录任务执行情况
   * @param spark
   */
  def create_table_spark_task_scheduling_info(spark:SparkSession,databaseName:String): Unit ={
    val sql =
      s"""
         |create table IF NOT EXISTS $databaseName.ods_spark_task_scheduling_info(
         | project_name String,
         | task_name String,
         | database_name String,
         | table_name String,
         | create_time String,
         | beging_time String,
         | end_time  String,
         | durationTime String,
         | begin_count String,
         | end_count String,
         | Difference String,
         | isSuccess String
         |)
         |stored as PARQUET
         |""".stripMargin
    spark.sql(sql)
  }


  /**
   * 从关系型数据库同步数据至hive表
   * @param dbsource_type
   * @param task_name
   * @param spark
   * @param hivedatabase
   * @param real_tableName
   * @param customize_tableName
   * @param dataFrame_temp_view
   * @param isNeedPartition
   * @param db_ip
   * @param db_port
   * @param db_name
   * @param user_name
   * @param user_password
   */
  def load_relationDbTable_to_hiveTable(dbsource_type:String,task_name:String,spark: SparkSession,
                                        hivedatabase:String,real_tableName:String,hive_table_name:String,
                                        customize_tableName:String,dataFrame_temp_view:String,
                                        isNeedPartition:String,db_ip:String,db_port:String,
                                        db_name:String,user_name:String,user_password:String): Unit ={

      //查询同步时表总数，并记录此时的时间戳
      val beging_time = System.currentTimeMillis()
      //根据传入值加载不同的数据源
      val prop:Properties = CreateDadtabaseInfoUtils.create_relation_db_properties_info(dbsource_type,db_ip,db_port,db_name,user_name,user_password)
      val url = prop.getProperty("url")
      val tablecnt = CreateDadtabaseInfoUtils.findRelationTableCntInfo(dbsource_type,real_tableName,prop)
      //判断是否需要分区，主要是根据数据量大小来判断
      val array: Array[String] = CreateDadtabaseInfoUtils.checkIsNeedSparkJdbcPredicates(tablecnt,isNeedPartition)
      var isNeed = false
      if( array != null && array.length > 0){
         isNeed = true
      }
      //开始同步数据，并将数据保存至hiv表中
      insert_into_table(spark,url, customize_tableName,prop,dataFrame_temp_view, array,hivedatabase,isNeed,hive_table_name)

      //同步结束后再一次执行查询关系型数据库的数据总量，便于误差对比
      val end_count = CreateDadtabaseInfoUtils.findRelationTableCntInfo(dbsource_type,real_tableName,prop)

      //保存执行结果，便于后续问题排查
      val end_time = System.currentTimeMillis()
      val project_name = "hftDataWarehouse"
      insert_table_spark_task_scheduling_info(project_name,task_name,hivedatabase,beging_time,end_time,hive_table_name,tablecnt,end_count,spark)
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
    val insert_sql = "insert into table "+hiveDatabase+"."+hiveTableName+" select * from global_temp."+tableNameView
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
