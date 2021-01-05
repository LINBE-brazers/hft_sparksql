package hftspqrksql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.Config

object Hftspatksql {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = createSparkSession()

    val databaseName = "hft_database"
    //创建数据库
    create_database_info(spark,databaseName)

    //连接数据库配置
    val prop = create_db_properties_info()
    val url_sqlserver = Config.getProperty("url_sqlserver")

    //同步v_ach_sta_day
    val client_building_relation_sqlserver = "(select row_number() over(order by id) as idNumber,a.* from client_building_relation a) temptable "
    val client_building_relation = "client_building_relation"
    val client_building_relation_view = "client_building_relation_view"
    create_table_client_building_relation(spark)
    insert_into_table(spark,url_sqlserver,client_building_relation_sqlserver,prop,client_building_relation_view,client_building_relation)


  }


  def create_table_client_building_relation(spark:SparkSession): Unit ={
    val sql =
      """
        |create table IF NOT EXISTS hft_database.client_building_relation(
        |  idNumber int,
        |	 id   String,
        |	 broker_id   String,
        |	 client_id   String,
        |	 building_id   String,
        |	 client_status   String,
        |	 is_locked   String,
        |	 locked_at   String,
        |	 unlocked_at   String,
        |	 is_visited   String,
        |	 is_signed   String,
        |	 protect_time   String,
        |	 creater   String,
        |	 create_time   String,
        |	 remark   String,
        |	 city_id   String,
        |	 is_remind   String,
        |	 mingyuan_status   String,
        |	 exhibition_room_id   String,
        |	 client_building_id   String,
        |	 visite_company   String,
        |	 visite_company_id   String,
        |	 visite_place   String,
        |	 visite_place_id   String,
        |	 client_name   String,
        |	 is_id_card   String,
        |	 client_id_card   String,
        |	 activate_sign   String,
        |	 bespeak_time   String,
        |	 source_id   String,
        |	 time_stamp   String,
        |	 recommend_source   String,
        |	 platform   String,
        |	 health_intention   String,
        |	 employee_mark   String,
        |	 is_black   String,
        |	 org_id   String,
        |	 org_name   String,
        |	 user_type   String,
        |	 parent_user_type   String,
        |	 update_time   String,
        |	 is_special   String,
        |	 owner_flag   String,
        |	 is_h5_delete   String,
        |	 is_employee_et   String,
        |	 is_tuoke   String,
        |	 client_phone   String,
        |	 is_make_up   String,
        |	 property_type   String,
        |	 is_signing   String,
        |	 member_level   String,
        |	 member_level_reward  String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  hft_database.client_building_relation")
  }





  /**
   * 保存数据到对应的表中
   * @param spark
   * @param url
   * @param mysqlTableName
   * @param prop
   * @param tableNameView
   * @param hiveTableName
   */
  def insert_into_table(spark:SparkSession,url:String,
                        mysqlTableName:String,prop:Properties,
                        tableNameView:String,hiveTableName:String):Unit={
    val predicates: Array[String] = Array[String](" idNumber <= 5000000 ", " idNumber >5000000 and idNumber <= 10000000 "," idNumber >10000000 and idNumber <= 15000000 "," idNumber >10000000 and idNumber <= 15000000 "," idNumber >15000000 and idNumber <= 20000000 "," idNumber >20000000 and idNumber <= 25000000 "," idNumber >25000000 and idNumber <= 30000000 "," idNumber >30000000 and idNumber <= 35000000 "," idNumber >35000000 and idNumber <= 40000000 "," idNumber >40000000 and idNumber <= 45000000 "," idNumber >45000000 and idNumber <= 50000000 "," idNumber >50000000 and idNumber <= 55000000 "," idNumber >55000000 and idNumber <= 60000000 "," idNumber >60000000 and idNumber <= 65000000 "," idNumber >65000000 and idNumber <= 70000000 "," idNumber >70000000 and idNumber <= 75000000 "," idNumber >75000000 and idNumber <= 80000000 "," idNumber >80000000 and idNumber <= 85000000 "," idNumber >85000000 and idNumber <= 90000000 "," idNumber >90000000 and idNumber <= 95000000 "," idNumber >95000000 ")
    val df = spark.read.jdbc(url,mysqlTableName,predicates,prop)
    df.createOrReplaceGlobalTempView(tableNameView)
    val insert_sql = "insert into table hft_database."+hiveTableName+" select * from global_temp."+tableNameView
    spark.sql(insert_sql)
    spark.sqlContext.dropTempTable("global_temp."+tableNameView)
  }

  /**
   * 创建数据库
   * @param spark
   * @param databaseName
   */
  def create_database_info(spark:SparkSession,databaseName:String):Unit={
    spark.sql("create database IF NOT EXISTS "+databaseName)
  }

  /**
   * 创建连接关系型数据库的连接配置项
   * @return
   */
  def create_db_properties_info():Properties={
    val prop = new Properties()
    prop.put("username",Config.getProperty("user_name_sqlserver"))
    prop.put("password",Config.getProperty("passsword_sqlserver"))
    prop.put("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    prop
  }

  /**
   * 创建sparkSession
   * @return
   */
  def createSparkSession(): SparkSession ={
    val conf = create_spark_conf()
    val spark: SparkSession = SparkSession
      //构建SparkSession
      .builder()
      //启用hive支持
      .config(conf)
      .enableHiveSupport()
      //获取或者创建
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def create_spark_conf():SparkConf={
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName("Hftspatksql_project")
      .set("spark.sql.warehouse.dir", "/user/hive/warehouse")
      //设置并行度，默认200
      .set("spark.default.parallelism", "10")
      .set("spark.sql.shuffle.partitions", "10")
      //开启shuffle map端生产文件的合并
      .set("spark.shuffle.consolidateFiles", "true")
      //选取序列化机制，使用kyro，丢弃默认java序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //开启针对大查询sql的字节码编译，对于小查询不建议开启
      .set("spark.sql.codegen", "true")
      //开启shuffle数据的压缩
      .set("spark.shuffle.compress", "true")
      //增大shuffle map端缓冲区的大小，默认32k
      .set("spark.shuffle.file.buffer", "256k")
      //开启数据本地化NODE_LOCAL级别等待时间，默认3秒，根据日志增减
      .set("spark.locality.wait.node", "6s")
      //开启数据本地化PROCESS_LOCAL级别等待时间，默认3秒，根据日志增减
      .set("spark.locality.wait.process", "6s")
      //jvm 内存的占比设定，默认0.6
      .set("spark.memory.fraction", "0.3")
      //设置executor的缓存内存的占比
      .set("spark.memory.storageFraction", "0.8")
    conf
  }

}
