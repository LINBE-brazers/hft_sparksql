package hftDataWarehouse.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

/**
 * 创建hive表的相关信息
 */
object CreateHFTHiveTableInfo {


  /**
   * 创建hive数据库的client_building_relation表
   * @param spark
   */
  def create_table_client_building_relation(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
        spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_client_building_relation")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_client_building_relation(
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
        |	 time_stamp   bigint,
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
        |	 member_level   int,
        |	 member_level_reward  int
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_client_building_relation")
  }


  /**
   * 创建hive数据库的building表
   * @param spark
   */
  def create_table_building(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_building")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_building(
        |idNumber int,
        |id   String,
        |name   String,
        |city   String,
        |area   String,
        |discount   String,
        |address   String,
        |type   String,
        |characteristic   String,
        |price_per_suite_scope   String,
        |fitment_level   String,
        |sale_start_at   String,
        |plan_suite   String,
        |building_area   String,
        |plot_ratio   String,
        |greening_rate   String,
        |property_type   String,
        |property_fee   String,
        |advantage   String,
        |brief_introduction   String,
        |is_recommand   String,
        |hotline   String,
        |ratio   String,
        |park_info   String,
        |address_x   String,
        |address_y   String,
        |traffic_cfg   String,
        |creater   String,
        |create_time   String,
        |is_hot   String,
        |price   String,
        |building_type   String,
        |sale_point   String,
        |orientation   String,
        |link_position   String,
        |discount_end_time   String,
        |is_publish   String,
        |property_company   String,
        |delivery_time   String,
        |property_rights   String,
        |is_delete   String,
        |order_index   int,
        |min_price   String,
        |mingyuan_project_id   String,
        |company_id   String,
        |is_nationalMaketing   String,
        |province   String,
        |is_repeatRecommand   String,
        |price_bak   String,
        |price_per_suite_scope_bak   String,
        |min_price_bak   String,
        |is_accept_recommend   String,
        |is_have_reward   String,
        |create_time1   String,
        |is_yuke_check   String,
        |is_id_card   String,
        |is_new_opening   String,
        |new_opening_sort   int,
        |opening_date   String,
        |recommand_num   int,
        |h5_url   String,
        |employee_recommendation_only   int,
        |is_health   String,
        |online_customer_pic   String,
        |online_customer_seat   String,
        |is_kaohe   String,
        |new_opening_type   String,
        |show_poster   String,
        |poster_update_time   String,
        |field_open_type   String,
        |field_open_time   String,
        |is_special   String,
        |education_resource   String,
        |medical_health   String,
        |shopping_mall   String,
        |other_cfg   String,
        |status   int,
        |search_keyword   String,
        |is_hot_group   String,
        |hot_index_num   int,
        |wechat_name   String,
        |wechat_code   String,
        |wechat_url   String,
        |isinstitutionscard   String,
        |is_show_online_order   String,
        |order_tip   String,
        |defaut_room_product_type   String,
        |important_tip   String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_building")
  }

  /**
   * 创建hive数据库的broker表
   * @param spark
   */
  def create_table_broker(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
       spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_broker")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_broker(
        |idNumber int,
        |id   String,
        | m_phone   String,
        | m_weixin   String,
        | m_qq   String,
        | account_type   String,
        | superior_id   String,
        | broker_type   String,
        | city   String,
        | is_disabled   int,
        | ios_device   String,
        | android_device   String,
        | screen_name   String,
        | password   String,
        | org_account   String,
        | create_time   String,
        | build_id   String,
        | dormant_status   String,
        | vocation   String,
        | active_time   String,
        | is_employee   String,
        | login_time   String,
        | source_from   String,
        | recommender_id   String,
        | province_id   String,
        | is_frozen   int,
        | frozen_start_time   String,
        | frozen_end_time   String,
        | register_from   String,
        | is_illegal   String,
        | time_stamp   bigint,
        | is_cheatbroker   String,
        | register_ip   String,
        | mini_openId   String,
        | org_id   String,
        | org_name   String,
        | plaintext_pwd   String,
        | org_black_list_flag   String,
        | disabled_by   String,
        | union_id   String,
        | pwd_update_time   String,
        | gs_country   String,
        | gs_province   String,
        | gs_city   String,
        | gs_area   String,
        | gs_address   String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_broker")
  }

  /**
   * 创建ods_hft_sqlserver_broker_approve_backup
   * @param spark
   * @param databaseName
   * @param isDropTable
   */
  def create_table_broker_approve_backup(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_broker_approve_backup")
    }
    val sql =
      s"""
         |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_broker_approve_backup(
         |idNumber int,
         |id String,
         |broker_id String,
         |name String,
         |gender String,
         |bank_no String,
         |bank_name String,
         |idcard_img String,
         |idcard_neg_img String,
         |photo String,
         |update_time String,
         |approve_state String,
         |unapprove_reason String,
         |idcard_num String,
         |org_full_name String,
         |business_license String,
         |org_code String,
         |legal_name String,
         |legal_gender String,
         |legal_birth String,
         |legal_idcard String,
         |agent_name String,
         |agent_gender String,
         |agent_birth String,
         |agent_idcard String,
         |company_bank_no String,
         |company_logo String,
         |photos String,
         |legal_idcard_img String,
         |legal_idcard_neg_img String,
         |agent_idcard_img String,
         |agent_idcard_neg_img String,
         |agent_book String,
         |is_disabled String,
         |phone String,
         |apply_time String,
         |approve_time String,
         |weiChat String,
         |other_reason String,
         |zhi_fu_bao String,
         |pay_type String,
         |user_type String,
         |ems String,
         |bank_province String,
         |id_type String,
         |attribution_project String,
         |owner_flag String
         |)
         |stored as PARQUET
         |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_broker_approve_backup")
  }

  /**
   * 创建bi_kh_sales
   * @param spark
   * @param databaseName
   * @param isDropTable
   */
  def create_table_bi_kh_sales(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_bi_kh_sales")
    }
    val sql =
      s"""
         |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_bi_kh_sales(
         |company String,
         |manager_group String,
         |manager_name String,
         |manager_phone String,
         |manager_idcard String,
         |name String,
         |phone String,
         |idcard String,
         |is_manager_register String,
         |apply_time String,
         |member_type String,
         |manager_brokerid String,
         |broker_id String,
         |create_time String,
         |chongfu int
         |)
         |stored as PARQUET
         |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_bi_kh_sales")
  }

  /**
   * 创建bi_kh_owner
   * @param spark
   * @param databaseName
   * @param isDropTable
   */
  def create_table_bi_kh_owner(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_bi_kh_owner")
    }
    val sql =
      s"""
         |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_bi_kh_owner(
         |company String,
         |manager_group String,
         |manager_name String,
         |manager_phone String,
         |manager_idcard String,
         |name String,
         |phone String,
         |idcard String,
         |is_manager_register String,
         |apply_time String,
         |suoshuloupan String,
         |regou_time String,
         |manager_brokerid String,
         |broker_id String,
         |create_time String,
         |chongfu int
         |)
         |stored as PARQUET
         |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_bi_kh_owner")
  }


  /**
   * 创建bi_kh_manager_2
   * @param spark
   * @param databaseName
   * @param isDropTable
   */
  def create_table_bi_kh_manager_2(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_bi_kh_manager_2")
    }
    val sql =
      s"""
         |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_bi_kh_manager_2(
         |orgName String,
         |depName String,
         |mgrName String,
         |mgrPhone String,
         |mgrCardNo String,
         |memName String,
         |memPhone String,
         |label String,
         |isNew String,
         |dataSource String,
         |sourceType String,
         |createTime String,
         |broker_id String,
         |mgr_broker_id String,
         |memtype  int
         |)
         |stored as PARQUET
         |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_bi_kh_manager_2")
  }


  /**
   * 创建share_info
   * @param spark
   * @param databaseName
   * @param isDropTable
   */
  def create_table_share_info(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_share_info")
    }
    val sql =
      s"""
         |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_share_info(
         |idNumber int,
         |id	String,
         |share_user_id	 String,
         |share_user_type	 int	,
         |share_source_id	 String,
         |share_source_type	int	,
         |create_time	String
         |)
         |stored as PARQUET
         |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_share_info")
  }



  /**
   * 创建hive数据库的client_building_zygw表
   * @param spark
   */
  def create_table_client_building_zygw(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_client_building_zygw")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_client_building_zygw(
        | idNumber int,
        | id   String,
        | recommend_id   String,
        | zygw_name   String,
        | zygw_phone   String,
        | is_call_client   String,
        | create_time   String,
        | update_time   String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_client_building_zygw")
  }



  /**
   * 创建hive数据库的client_building_order表
   * @param spark
   */
  def create_table_client_building_order(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_client_building_order")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_client_building_order(
        | idNumber int,
        | id   String,
        | cont_tran_guid   String,
        | proj_guid   String,
        | cst_name   String,
        | card_id   String,
        | cst_mobile   String,
        | room_number   String,
        | turnover_area   String,
        | turnover_amount   String,
        | subs_time   String,
        | lifecycle_id   String,
        | create_time   String,
        | is_tran_fund   String,
        | time_stamp   bigint,
        | intention_rate   String,
        | propertyType   String,
        | MaxCommissionDeductionRatio   String,
        | roomguid   String,
        | member_level int,
        | user_type String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_client_building_order")
  }


  /**
   * 创建hive数据库的client_building_visited表
   * @param spark
   */
  def create_table_client_building_visited(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_client_building_visited")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_client_building_visited(
        | idNumber int,
        | id   String,
        | client_id   String,
        | building_id   String,
        | status   String,
        | signSeeHouse   String,
        | visited_time   String,
        | reception   String,
        | creater   String,
        | create_time   String,
        | remark   String,
        | lifecycle_id   String,
        | recheck_user   String,
        | recheck_mark   String,
        | visit_review_status   int,
        | purpose   String,
        | withSeeHouse   String,
        | my_visited_guid   String,
        | my_project_guid   String,
        | my_mobile   String,
        | is_exchange   String,
        | time_stamp   bigint,
        | is_local_visit   String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_client_building_visited")
  }

  /**
   * 创建hive数据库的customer_online_order_info表
   * @param spark
   */
  def create_table_customer_online_order_info(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_customer_online_order_info")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_customer_online_order_info(
        |idNumber int,
        |id String,
        |property_plan_no String,
        |order_num String,
        |project_guid String,
        |project_name String,
        |room_guid String,
        |room_name String,
        |client_name String,
        |client_phone String,
        |client_idcard String,
        |client_address String,
        |floor_area String,
        |floor_unit_price String,
        |decoration_standard String,
        |payment_method String,
        |turnover_amount decimal(20,2),
        |zygu_name String,
        |zygu_phone String,
        |money_name String,
        |money_amount decimal(20,2),
        |status String,
        |create_time String,
        |payment_deadline String,
        |update_time String,
        |is_delete String,
        |house_area String,
        |house_unit_price String,
        |order_pdf_url String,
        |order_image_url String,
        |order_sign_time String,
        |decoration_price decimal(20,2),
        |customer_id String,
        |mail String,
        |is_used String,
        |is_release String,
        |release_type String,
        |is_discount String,
        |is_from_yunke String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_customer_online_order_info")
  }


  /**
   * 创建hive数据库的customer_online_order_pay_info表
   * @param spark
   */
  def create_table_customer_online_order_pay_info(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_customer_online_order_pay_info")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_customer_online_order_pay_info(
        | idNumber int,
        | id   String,
        | order_id   String,
        | order_num   String,
        | room_status   String,
        | merchant_num   String,
        | merchant_wechat   String,
        | merchant_bank_name   String,
        | merchant_bank_num   String,
        | pay_method   String,
        | pay_time   String,
        | pay_amout   decimal(20,2),
        | create_time   String,
        | update_time   String,
        | zjAccountProjBuName   String,
        | zjAccountProjBuAdd   String,
        | zjAccountProjBuPhone   String,
        | zjAccountUse   String,
        | zjAccountSignatureID   String,
        | is_pay_success   String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_customer_online_order_pay_info")
  }


  /**
   * 创建hive数据库的sys_city表
   * @param spark
   */
  def create_table_sys_city(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_sys_city")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_sys_city(
        | idNumber int,
        | id   String,
        | name   String,
        | province_id   String,
        | area_code   String,
        | CompanyName   String,
        | city_code   String,
        | city_status   String,
        | city_x   String,
        | city_y   String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_sys_city")
  }


  /**
   * 创建hive数据库的sys_province表
   * @param spark
   */
  def create_table_sys_province(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_sys_province")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_sys_province(
        | idNumber int,
        | id   String,
        | name   String,
        | sort   int,
        | is_disabled   int,
        | alias   String,
        | phone   String,
        | province_x   String,
        | province_y   String,
        | is_special   String,
        | code   String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_sys_province")
  }

  /**
   * 创建hive数据库的broker_member_level表
   * @param spark
   */
  def create_table_broker_member_level(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_broker_member_level")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_broker_member_level(
        | idNumber int,
        | id   String,
        | broker_id   String,
        | broker_level   int,
        | visit_num   int,
        | order_num   int,
        | create_time   String,
        | update_time   String,
        | is_drop_level   String,
        | broker_drop_level   int,
        | flag   String,
        | is_manager_member   int,
        | is_kh_employee   int,
        | higher_broker_id String,
        | binding_time  String,
        | approve_ownerFlag String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_broker_member_level")
  }

  /**
   * 创建hive数据库的broker_province表
   * @param spark
   */
  def create_table_broker_province(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_broker_province")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_broker_province(
        | idNumber int,
        | id   String,
        | province_code   String,
        | province_name   String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_broker_province")
  }


  /**
   * 创建hive数据库的broker_city表
   * @param spark
   */
  def create_table_broker_city(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_broker_city")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_broker_city(
        | idNumber int,
        | id   String,
        | province_id   String,
        | city_code   String,
        | city_name   String,
        | province_code   String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_broker_city")
  }


  /**
   * 创建hive数据库的broker_area表
   * @param spark
   */
  def create_table_broker_area(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_broker_area")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_broker_area(
        | idNumber int,
        | id   String,
        | city_id   String,
        | area_code   String,
        | area_name   String,
        | city_code   String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_broker_area")
  }


  /**
   * 创建hive数据库的bi_manager_member_relation表
   * @param spark
   */
  def create_table_bi_manager_member_relation(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_bi_manager_member_relation")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_bi_manager_member_relation(
        | `序号`   String,
        | province   String,
        | screen_name   String,
        | m_phone   String,
        | user_type   String,
        | project_name   String,
        | `是否已交楼`   String,
        | `员工单位`   String,
        | `拓客员归属项目`   String,
        | member_level   String,
        | manager_idcard   String,
        | manager_sap   String,
        | data   String,
        | manager_phone   String,
        | employee_time   String,
        | date   String,
        | remark   String
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_bi_manager_member_relation")
  }

  /**
   * 创建hive数据库的locate_info表
   * @param spark
   */
  def create_table_locate_info(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
       spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_locate_info")
    }
    val sql =
      s"""
        |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_locate_info(
        |idNumber int,
        |id	string,
        |broker_id	string,
        |device_info	string,
        |device_type	int,
        |longitude	string,
        |latitude	string,
        |country	string,
        |province	string,
        |city	string,
        |address	string,
        |create_time	string
        |)
        |stored as PARQUET
        |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_locate_info")
  }

  /**
   * 创建hive数据库的broker_approve表
   * @param spark
   */
  def create_table_broker_approve(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
       spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_broker_approve")
    }
    val sql =
      s"""
         |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_broker_approve(
         | idNumber int,
         | id   String,
         | broker_id   String,
         | name   String,
         | gender   String,
         | bank_no   String,
         | bank_name   String,
         | idcard_img   String,
         | idcard_neg_img   String,
         | photo   String,
         | update_time   String,
         | approve_state   String,
         | unapprove_reason   String,
         | idcard_num   String,
         | org_full_name   String,
         | business_license   String,
         | org_code   String,
         | legal_name   String,
         | legal_gender   String,
         | legal_birth   String,
         | legal_idcard   String,
         | agent_name   String,
         | agent_gender   String,
         | agent_birth   String,
         | agent_idcard   String,
         | company_bank_no   String,
         | company_logo   String,
         | photos   String,
         | legal_idcard_img   String,
         | legal_idcard_neg_img   String,
         | agent_idcard_img   String,
         | agent_idcard_neg_img   String,
         | agent_book   String,
         | is_disabled   String,
         | phone   String,
         | apply_time   String,
         | approve_time   String,
         | approve_userid   String,
         | e_mail   String,
         | m_qq   String,
         | weiChat   String,
         | other_reason   String,
         | zhi_fu_bao   String,
         | pay_type   String,
         | user_type   String,
         | ems   String,
         | bank_province   String,
         | id_type   String,
         | attribution_project   String,
         | time_stamp   bigint,
         | owner_flag   String,
         | IDCard_start_time   String,
         | IDCard_end_time   String,
         | photo_type   String,
         | is_employee_et   String,
         | sn_city_id   String,
         | sn_employee_code   String
         |)
         |stored as PARQUET
         |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_broker_approve")
  }


  /**
   * 创建hive数据库的bi_kh_manager表,
   * @param spark
   */
  def create_table_bi_kh_manager(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_bi_kh_manager")
    }
    val sql =
      s"""
         |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_bi_kh_manager(
         |`地区公司` String,
         |`会员经理所属部门` String,
         |`会员经理姓名` String,
         |`会员经理手机号码` String,
         |`会员经理身份证号` String,
         |`会员经理发展的会员姓名` String,
         |`会员经理发展的会员手机号码` String,
         |`是否新发展` String,
         |remark  String,
         |`会员broker_id` String,
         |`会员经理broker_id` String,
         |memtype int
         |)
         |stored as PARQUET
         |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_bi_kh_manager")
  }

  /**
   * 创建hive数据库的bi_kh_employee表,
   * @param spark
   */
  def create_table_bi_kh_employee(spark:SparkSession,databaseName:String,isDropTable:String): Unit ={
    if(StringUtils.equals(ConstantUtils.IS_DROP_TABLE,isDropTable)){
      spark.sql("drop table if exists  "+databaseName+".ods_hft_sqlserver_bi_kh_employee")
    }
    val sql =
      s"""
         |create table IF NOT EXISTS $databaseName.ods_hft_sqlserver_bi_kh_employee(
         | sap   String,
         |`姓名`   String,
         |`地区公司`   String,
         |`会员经理所属部门`   String,
         |`会员经理身份证号`   String
         |)
         |stored as PARQUET
         |""".stripMargin
    spark.sql(sql)
    spark.sql("truncate table  "+databaseName+".ods_hft_sqlserver_bi_kh_employee")
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

}