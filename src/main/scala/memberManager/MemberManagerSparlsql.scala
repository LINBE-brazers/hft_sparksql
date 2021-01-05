package memberManager

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utils.Config

object MemberManagerSparlsql {


  /**
   * sparksql程序函数主入口
   * @param args
   */
  def main(args: Array[String]): Unit = {

      //创建SparkSession
      val spark = createSparkSession()

      val databaseName = "member_database"
      //创建数据库
      create_database_info(spark,databaseName)

      //连接数据库配置
      val prop = create_db_properties_info()
      val url_mysql = Config.getProperty("url_mysql")

      //同步v_ach_sta_day
      val mysqlTableName1 = "v_ach_sta_day"
      val sparkTmpView1 = "v_ach_sta_day_view"
      create_table_v_ach_sta_day(spark)
      insert_into_table(spark,url_mysql,mysqlTableName1,prop,sparkTmpView1,mysqlTableName1)

      //同步v_ach_sta_month
      val mysqlTableName2 = "v_ach_sta_month"
      val sparkTmpView2 = "v_ach_sta_month_view"
      create_table_v_ach_sta_month(spark)
      insert_into_table(spark,url_mysql,mysqlTableName2,prop,sparkTmpView2,mysqlTableName2)

      //同步v_ach_sta_week
      val mysqlTableName3 = "v_ach_sta_week"
      val sparkTmpView3 = "v_ach_sta_week_view"
      create_table_v_ach_sta_week(spark)
      insert_into_table(spark,url_mysql,mysqlTableName3,prop,sparkTmpView3,mysqlTableName3)


      //同步v_base_manager
      val mysqlTableName4 = "v_base_manager"
      val sparkTmpView4 = "v_base_manager_view"
      create_table_v_base_manager(spark)
      insert_into_table(spark,url_mysql,mysqlTableName4,prop,sparkTmpView4,mysqlTableName4)

      //同步v_manager_org
      val mysqlTableName5 = "v_manager_org"
      val sparkTmpView5 = "v_manager_org_view"
      create_table_v_manager_org(spark)
      insert_into_table(spark,url_mysql,mysqlTableName5,prop,sparkTmpView5,mysqlTableName5)

      //同步v_member_grade_verify
      val mysqlTableName6 = "v_member_grade_verify"
      val sparkTmpView6 = "v_member_grade_verify_view"
      create_table_v_member_grade_verify(spark)
      insert_into_table(spark,url_mysql,mysqlTableName6,prop,sparkTmpView6,mysqlTableName6)


      //同步v_organization
      val mysqlTableName7 = "v_organization"
      val sparkTmpView7 = "v_organization_view"
      create_table_v_organization(spark)
      insert_into_table(spark,url_mysql,mysqlTableName7,prop,sparkTmpView7,mysqlTableName7)

      //同步v_base_member
      val mysqlTableName8 = "v_base_member"
      val sparkTmpView8 = "v_base_member_view"
      create_table_v_base_member(spark)
      insert_into_table(spark,url_mysql,mysqlTableName8,prop,sparkTmpView8,mysqlTableName8)

    //同步v_hdb_svc_province_city_area_company
    val mysqlTableName9 = "v_hdb_svc_province_city_area_company"
    val sparkTmpView9 = "v_hdb_svc_province_city_area_company_view"
    create_table_v_hdb_svc_province_city_area_company(spark)
    insert_into_table(spark,url_mysql,mysqlTableName9,prop,sparkTmpView9,mysqlTableName9)


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
    val df = spark.read.jdbc(url,mysqlTableName,prop)
    df.createOrReplaceGlobalTempView(tableNameView)
    val insert_sql = "insert into table member_database."+hiveTableName+" select * from global_temp."+tableNameView
    spark.sql(insert_sql)
    spark.sqlContext.dropTempTable("global_temp."+tableNameView)
  }

  def create_table_v_hdb_svc_province_city_area_company(spark:SparkSession):Unit={
    val sql =
      """
        |create table IF NOT EXISTS  member_database.v_hdb_svc_province_city_area_company(
        |id  int,
        |name String,
        |company String
        |)
        |stored as PARQUET
      """.stripMargin
    spark.sql(sql)
    spark.sql("truncate table  member_database.v_hdb_svc_province_city_area_company")
  }


  def create_table_v_base_manager(spark:SparkSession):Unit={
    val sql =
      """
        |create table IF NOT EXISTS  member_database.v_base_manager(
        |id           bigint,
        |hft_id       string,
        |name         string,
        |id_card_type string,
        |id_card_no   string,
        |phone        string,
        |entry_time   timestamp,
        |create_time  timestamp,
        |update_time  timestamp,
        |is_disabled  int,
        |is_cleaned   int
        |)
        |stored as PARQUET
      """.stripMargin
    spark.sql(sql)
    spark.sql("truncate table  member_database.v_base_manager")
  }

  def create_table_v_manager_org(spark:SparkSession):Unit={
      val sql =
        """
          |create table IF NOT EXISTS  member_database.v_manager_org(
          |id          bigint,
          |mgr_id      bigint,
          |org_id      bigint,
          |is_disabled int
          |)
          |stored as PARQUET
        """.stripMargin
      spark.sql(sql)
      spark.sql("truncate table  member_database.v_manager_org")
  }

  def create_table_v_base_member(spark:SparkSession):Unit={
    val sql =
      """
        |create table IF NOT EXISTS  member_database.v_base_member(
        |id  bigint,
        |hft_id  String,
        |mgr_hft_id String,
        |name   String,
        |phone     String,
        |id_card_type   String,
        |id_card_no    String,
        |data_source   int,
        |create_time     String,
        |update_time     String,
        |is_disabled   int,
        |assign_status int,
        |logout_acc   int,
        |label       int
        |)
        |stored as PARQUET
        """.stripMargin
    spark.sql(sql)
    spark.sql("truncate table  member_database.v_base_member")
  }

  def create_table_v_member_grade_verify(spark:SparkSession):Unit={
    val sql =
      """
        |create table IF NOT EXISTS  member_database.v_member_grade_verify(
        |id                 bigint,
        |m_id               string,
        |m_grade            int,
        |state              int,
        |broker_type        string,
        |update_time        string,
        |insert_time        string,
        |approve_time       string,
        |approve_by         string,
        |user_type          string,
        |work_org_name      string,
        |working_place      string,
        |working_place_name string,
        |work_year          int,
        |recommender_name   string,
        |recommender_id     string,
        |card_pic           string,
        |other_pic          string,
        |m_desc             string,
        |approve_desc       string,
        |approve_by_id      int,
        |m_name             string,
        |work_pic String,
        |memo  String,
        |gmt_created String,
        |purchase_item  String,
        |phone String,
        |source_type int,
        |label int
        |)
        |stored as PARQUET
        """.stripMargin
    spark.sql(sql)
    spark.sql("truncate table  member_database.v_member_grade_verify")
  }

  def create_table_v_organization(spark:SparkSession):Unit={
    val sql =
      """
        |create table IF NOT EXISTS  member_database.v_organization(
        |id         bigint,
        |parent_id  bigint,
        |name       string,
        |path       string,
        |level      int
        |)
        |stored as PARQUET
        """.stripMargin
    spark.sql(sql)
    spark.sql("truncate table  member_database.v_organization")
  }




  def create_table_v_ach_sta_day(spark:SparkSession):Unit={
    val sql =
      """
        |create table IF NOT EXISTS  member_database.v_ach_sta_day(
        |id              int,
        |broker_id       string,
        |recommender_id  string,
        |phone           string,
        |name            string,
        |resident        string,
        |birthday        string,
        |cur_date        timestamp,
        |share_poster    int,
        |client_browse   int,
        |recommend       int,
        |visited         int,
        |subscribed      int,
        |signed          int,
        |invite_reg      int,
        |create_time     timestamp,
        |manager_id      bigint,
        |manager_name    string,
        |manager_hft_id  string,
        |department_id   bigint,
        |department_name string,
        |company_id      bigint,
        |company_name    string,
        |authed          int,
        |un_authed       int,
        |update_time     timestamp
        |)
        |stored as PARQUET
      """.stripMargin
    spark.sql(sql)
    spark.sql("truncate table  member_database.v_ach_sta_day")
  }

  def create_table_v_ach_sta_month(spark:SparkSession):Unit={
    val sql =
      """
        |create table IF NOT EXISTS  member_database.v_ach_sta_month(
        |id              int,
        |broker_id       string,
        |recommender_id  string,
        |phone           string,
        |name            string,
        |resident        string,
        |birthday        string,
        |cur_date        timestamp,
        |share_poster    int,
        |client_browse   int,
        |recommend       int,
        |visited         int,
        |subscribed      int,
        |signed          int,
        |invite_reg      int,
        |create_time     timestamp,
        |manager_id      bigint,
        |manager_name    string,
        |manager_hft_id  string,
        |department_id   bigint,
        |department_name string,
        |company_id      bigint,
        |company_name    string,
        |authed          int,
        |un_authed       int,
        |update_time     timestamp
        |)
        |stored as PARQUET
      """.stripMargin
    spark.sql(sql)
    spark.sql("truncate table  member_database.v_ach_sta_month")
  }

  def create_table_v_ach_sta_week(spark:SparkSession):Unit={
    val sql =
      """
        |create table IF NOT EXISTS  member_database.v_ach_sta_week(
        |id              int,
        |broker_id       string,
        |recommender_id  string,
        |phone           string,
        |name            string,
        |resident        string,
        |birthday        string,
        |monday_date     timestamp,
        |sunday_date     timestamp,
        |share_poster    int,
        |client_browse   int,
        |recommend       int,
        |visited         int,
        |subscribed      int,
        |signed          int,
        |invite_reg      int,
        |create_time     timestamp,
        |manager_id      bigint,
        |manager_name    string,
        |manager_hft_id  string,
        |department_id   bigint,
        |department_name string,
        |company_id      bigint,
        |company_name    string,
        |authed          int,
        |un_authed       int,
        |update_time     timestamp
        |)
        |stored as PARQUET
      """.stripMargin
    spark.sql(sql)
    spark.sql("truncate table  member_database.v_ach_sta_week")
  }


  /**
   * 创建spark_conf的基本配置
   * @return
   */
  def create_spark_conf():SparkConf={
     val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("MemberManagerSparlsql_project")
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
    prop.put("user",Config.getProperty("user_name_mysql"))
    prop.put("password",Config.getProperty("passsword_mysql"))
    prop.put("driver","com.mysql.jdbc.Driver")
    prop
  }


}
