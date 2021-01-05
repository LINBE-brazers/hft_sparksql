package hftDataWarehouse.utils

/**
 * 查询关系型DB库的表字段，如果此处字段减少，插入hive表字段也需对应减少
 */
object SelectRelectionDBTableUtils {


  /**
   * client_building_relation表查询语句
   * @return
   */
  def select_client_building_relation():  String={
    val sql =
      """
        |  (select row_number() over(order by id) as idNumber,
        |	 id   ,
        |	 broker_id   ,
        |	 client_id   ,
        |	 building_id   ,
        |	 client_status   ,
        |	 is_locked   ,
        |	 locked_at   ,
        |	 unlocked_at   ,
        |	 is_visited   ,
        |	 is_signed   ,
        |	 protect_time   ,
        |	 creater   ,
        |	 create_time   ,
        |	 remark   ,
        |	 city_id   ,
        |	 is_remind   ,
        |	 mingyuan_status   ,
        |	 exhibition_room_id   ,
        |	 client_building_id   ,
        |	 visite_company   ,
        |	 visite_company_id   ,
        |	 visite_place   ,
        |	 visite_place_id   ,
        |	 client_name   ,
        |	 is_id_card   ,
        |	 client_id_card   ,
        |	 activate_sign   ,
        |	 bespeak_time   ,
        |	 source_id   ,
        |	 CONVERT(BIGINT,time_stamp) as time_stamp_new,
        |	 recommend_source,
        |	 platform   ,
        |	 health_intention   ,
        |	 employee_mark   ,
        |	 is_black   ,
        |	 org_id   ,
        |	 org_name   ,
        |	 user_type   ,
        |	 parent_user_type   ,
        |	 update_time   ,
        |	 is_special   ,
        |	 owner_flag   ,
        |	 is_h5_delete   ,
        |	 is_employee_et   ,
        |	 is_tuoke   ,
        |	 client_phone   ,
        |	 is_make_up   ,
        |	 property_type   ,
        |	 is_signing   ,
        |	 member_level   ,
        |	 member_level_reward  from  client_building_relation) sparksql_client_building_relation
        |""".stripMargin
    sql
  }


  /**
   *building表查询字段
   */
  def select_building():  String={
    val sql =
      """
        |(select row_number() over(order by id) as idNumber,
        |id,
        |name,
        |city,
        |area,
        |discount,
        |address,
        |type,
        |characteristic,
        |price_per_suite_scope,
        |fitment_level,
        |sale_start_at,
        |plan_suite,
        |building_area,
        |plot_ratio,
        |greening_rate,
        |property_type,
        |property_fee,
        |advantage,
        |brief_introduction,
        |is_recommand,
        |hotline,
        |ratio,
        |park_info,
        |address_x,
        |address_y,
        |traffic_cfg,
        |creater,
        |create_time,
        |is_hot,
        |price,
        |building_type,
        |sale_point,
        |orientation,
        |link_position,
        |discount_end_time,
        |is_publish,
        |property_company,
        |delivery_time,
        |property_rights,
        |is_delete,
        |order_index,
        |min_price,
        |mingyuan_project_id,
        |company_id,
        |is_nationalMaketing,
        |province,
        |is_repeatRecommand,
        |price_bak,
        |price_per_suite_scope_bak,
        |min_price_bak,
        |is_accept_recommend,
        |is_have_reward,
        |create_time1,
        |is_yuke_check,
        |is_id_card,
        |is_new_opening,
        |new_opening_sort,
        |opening_date,
        |recommand_num,
        |h5_url,
        |employee_recommendation_only,
        |is_health,
        |online_customer_pic,
        |online_customer_seat,
        |is_kaohe,
        |new_opening_type,
        |show_poster,
        |poster_update_time,
        |field_open_type,
        |field_open_time,
        |is_special,
        |education_resource,
        |medical_health,
        |shopping_mall,
        |other_cfg,
        |status,
        |search_keyword,
        |is_hot_group,
        |hot_index_num,
        |wechat_name,
        |wechat_code,
        |wechat_url,
        |isinstitutionscard,
        |is_show_online_order,
        |order_tip,
        |defaut_room_product_type,
        |important_tip from building) sparksql_building
        |""".stripMargin
    sql
  }

  /**
   * 查询broker
   * @return
   */
  def select_broker():  String={
    val sql =
      """
        |(select row_number() over(order by id) as idNumber,
        | id   ,
        | m_phone   ,
        | m_weixin   ,
        | m_qq   ,
        | account_type   ,
        | superior_id   ,
        | broker_type   ,
        | city   ,
        | is_disabled   ,
        | ios_device   ,
        | android_device   ,
        | screen_name   ,
        | password   ,
        | org_account   ,
        | create_time   ,
        | build_id   ,
        | dormant_status   ,
        | vocation   ,
        | active_time   ,
        | is_employee   ,
        | login_time   ,
        | source_from   ,
        | recommender_id   ,
        | province_id   ,
        | is_frozen   ,
        | frozen_start_time ,
        | frozen_end_time ,
        | register_from ,
        | is_illegal   ,
        | CONVERT(BIGINT,time_stamp) as time_stamp_new,
        | is_cheatbroker   ,
        | register_ip   ,
        | mini_openId   ,
        | org_id   ,
        | org_name   ,
        | plaintext_pwd   ,
        | org_black_list_flag   ,
        | disabled_by   ,
        | union_id   ,
        | pwd_update_time   ,
        | gs_country   ,
        | gs_province   ,
        | gs_city   ,
        | gs_area   ,
        | gs_address  from broker) sparksql_broker
        |""".stripMargin
    sql
  }


  /**
   *创建broker_approve_backup表
   * @return
   */
  def select_broker_approve_backup():  String={
    val sql =
      """
        |(select row_number() over(order by id) as idNumber,
        |id,
        |broker_id,
        |name,
        |gender,
        |bank_no,
        |bank_name,
        |idcard_img,
        |idcard_neg_img,
        |photo,
        |update_time,
        |approve_state,
        |unapprove_reason,
        |idcard_num,
        |org_full_name,
        |business_license,
        |org_code,
        |legal_name,
        |legal_gender,
        |legal_birth,
        |legal_idcard,
        |agent_name,
        |agent_gender,
        |agent_birth,
        |agent_idcard,
        |company_bank_no,
        |company_logo,
        |photos,
        |legal_idcard_img,
        |legal_idcard_neg_img,
        |agent_idcard_img,
        |agent_idcard_neg_img,
        |agent_book,
        |is_disabled,
        |phone,
        |apply_time,
        |approve_time,
        |weiChat,
        |other_reason,
        |zhi_fu_bao,
        |pay_type,
        |user_type,
        |ems,
        |bank_province,
        |id_type,
        |attribution_project,
        |owner_flag from broker_approve_backup) sparksql_broker_approve_backup
        |""".stripMargin
    sql
  }

  /**
   *查询表
   * @return
   */
  def select_bi_kh_sales():  String={
    val sql =
      """
        |(select company,
        |manager_group,
        |manager_name,
        |manager_phone,
        |manager_idcard,
        |name,
        |phone,
        |idcard,
        |is_manager_register,
        |apply_time,
        |member_type,
        |manager_brokerid,
        |broker_id,
        |create_time,
        |chongfu from bi_kh_sales) sparksql_bi_kh_sales
        |""".stripMargin
    sql
  }

  /**
   *查询表
   * @return
   */
  def select_bi_kh_owner():  String={
    val sql =
      """
        |(select company,
        |manager_group,
        |manager_name,
        |manager_phone,
        |manager_idcard,
        |name,
        |phone,
        |idcard,
        |is_manager_register,
        |apply_time,
        |suoshuloupan,
        |regou_time,
        |manager_brokerid,
        |broker_id,
        |create_time,
        |chongfu from bi_kh_owner) sparksql_bi_kh_owner
        |""".stripMargin
    sql
  }


  /**
   *查询表bi_kh_manager_2
   * @return
   */
  def select_bi_kh_manager_2():  String={
    val sql =
      """
        |(select orgName,
        |depName,
        |mgrName,
        |mgrPhone,
        |mgrCardNo,
        |memName,
        |memPhone,
        |label,
        |isNew,
        |dataSource,
        |sourceType,
        |createTime,
        |broker_id,
        |mgr_broker_id,
        |memtype from bi_kh_manager_2) sparksql_bi_kh_manager_2
        |""".stripMargin
    sql
  }

  /**
   *查询表share_info
   * @return
   */
  def select_share_info():  String={
    val sql =
      """
        |(select row_number() over(order by id) as idNumber,
        |id,
        |share_user_id,
        |share_user_type,
        |share_source_id,
        |share_source_type,
        |create_time from share_info) sparksql_share_info
        |""".stripMargin
    sql
  }

  /**
   * 查询client_building_zygw字段
   */
  def select_client_building_zygw():  String={
    val sql =
      """
        |(select row_number() over(order by id) as idNumber,
        | id   ,
        | recommend_id   ,
        | zygw_name   ,
        | zygw_phone   ,
        | is_call_client   ,
        | create_time   ,
        | update_time  from  client_building_zygw) sparksql_client_building_zygw
        |""".stripMargin
    sql
  }



  /**
   * 查询client_building_order表
   */
  def select_client_building_order(): String ={
    val sql =
      """
	    | (select row_number() over(order by id) as idNumber,
      | id   ,
      | cont_tran_guid   ,
      | proj_guid   ,
      | cst_name   ,
      | card_id   ,
      | cst_mobile   ,
      | room_number   ,
      | turnover_area   ,
      | turnover_amount   ,
      | subs_time   ,
      | lifecycle_id   ,
      | create_time   ,
      | is_tran_fund   ,
      | CONVERT(BIGINT,time_stamp)  time_stamp_new,
      | intention_rate   ,
      | propertyType   ,
      | MaxCommissionDeductionRatio,
      | roomguid,
      | member_level,
      | user_type from  client_building_order) sparksql_client_building_order
      |""".stripMargin
    sql
  }


  /**
   * 查询client_building_visited表
   */
  def select_client_building_visited(): String ={
    val sql =
      """
	    | (select row_number() over(order by id) as idNumber,
      | id   ,
      | client_id   ,
      | building_id   ,
      | status   ,
      | signSeeHouse   ,
      | visited_time   ,
      | reception   ,
      | creater   ,
      | create_time   ,
      | remark   ,
      | lifecycle_id   ,
      | recheck_user   ,
      | recheck_mark   ,
      | visit_review_status   ,
      | purpose   ,
      | withSeeHouse   ,
      | my_visited_guid   ,
      | my_project_guid   ,
      | my_mobile   ,
      | is_exchange   ,
      | CONVERT(BIGINT,time_stamp) as time_stamp_new,
      | is_local_visit from client_building_visited) sparksql_client_building_visited
      |""".stripMargin
    sql
  }

  /**
   * 查询customer_online_order_info表
   */
  def select_customer_online_order_info(): String ={
    val sql =
      """
      |(select row_number() over(order by id) as idNumber,
      |id,
      |property_plan_no,
      |order_num,
      |project_guid,
      |project_name,
      |room_guid,
      |room_name,
      |client_name,
      |client_phone,
      |client_idcard,
      |client_address,
      |floor_area,
      |floor_unit_price,
      |decoration_standard,
      |payment_method,
      |turnover_amount,
      |zygu_name,
      |zygu_phone,
      |money_name,
      |money_amount,
      |status,
      |create_time,
      |payment_deadline,
      |update_time,
      |is_delete,
      |house_area,
      |house_unit_price,
      |order_pdf_url,
      |order_image_url,
      |order_sign_time,
      |decoration_price,
      |customer_id,
      |mail,
      |is_used,
      |is_release,
      |release_type,
      |is_discount ,
      |is_from_yunke from customer_online_order_info) sparksql_customer_online_order_info
      |""".stripMargin
    sql
  }


  /**
   * 查询customer_online_order_pay_info表
   */
  def select_customer_online_order_pay_info(): String ={
    val sql =
      """
        | (select row_number() over(order by id) as idNumber,
        | id   ,
        | order_id   ,
        | order_num   ,
        | room_status   ,
        | merchant_num   ,
        | merchant_wechat   ,
        | merchant_bank_name   ,
        | merchant_bank_num   ,
        | pay_method   ,
        | pay_time   ,
        | pay_amout  ,
        | create_time   ,
        | update_time   ,
        | zjAccountProjBuName   ,
        | zjAccountProjBuAdd   ,
        | zjAccountProjBuPhone   ,
        | zjAccountUse   ,
        | zjAccountSignatureID ,
        | is_pay_success  from customer_online_order_pay_info) sparksql_customer_online_order_pay_info
        |""".stripMargin
    sql
  }


  /**
   * 查询sys_city表
   */
  def select_sys_city(): String ={
    val sql =
      """
        | (select row_number() over(order by id) as idNumber,
        | id   ,
        | name   ,
        | province_id   ,
        | area_code   ,
        | CompanyName   ,
        | city_code   ,
        | city_status   ,
        | city_x   ,
        | city_y   from sys_city) sparksql_sys_city
        |""".stripMargin
    sql
  }


  /**
   * 查询sys_province表
   */
  def select_sys_province(): String ={
    val sql =
      """
        | (select row_number() over(order by id) as idNumber ,
        | id   ,
        | name   ,
        | sort   ,
        | is_disabled   ,
        | alias   ,
        | phone   ,
        | province_x   ,
        | province_y   ,
        | is_special   ,
        | code  from  sys_province) sparksql_sys_province
        |""".stripMargin
    sql
  }

  /**
   * 查询broker_member_level表
   */
  def select_broker_member_level(): String ={
    val sql =
      """
        | (select row_number() over(order by id) as idNumber ,
        | id   ,
        | broker_id   ,
        | broker_level   ,
        | visit_num   ,
        | order_num   ,
        | create_time   ,
        | update_time   ,
        | is_drop_level   ,
        | broker_drop_level ,
        | flag   ,
        | is_manager_member ,
        | is_kh_employee  ,
        | higher_broker_id,
        | binding_time,
        | approve_ownerFlag from broker_member_level) sparksql_broker_member_level
        |""".stripMargin
    sql
  }

  /**
   * 查询broker_province表
   */
  def select_broker_province(): String ={
    val sql =
      """
        | (select row_number() over(order by id) as idNumber,
        | id   ,
        | province_code   ,
        | province_name   from broker_province) sparksql_broker_province
        |""".stripMargin
    sql
  }


  /**
   * 查询broker_city表
   */
  def select_broker_city(): String ={
    val sql =
      """
        | (select row_number() over(order by id) as idNumber,
        | id   ,
        | province_id   ,
        | city_code   ,
        | city_name   ,
        | province_code   from broker_city) sparksql_broker_city
        |""".stripMargin
    sql
  }


  /**
   * 查询broker_area表
   */
  def select_broker_area(): String ={
    val sql =
      """
        | (select row_number() over(order by id) as idNumber,
        | id   ,
        | city_id   ,
        | area_code   ,
        | area_name   ,
        | city_code  from  broker_area) sparksql_broker_area
        |""".stripMargin
    sql
  }


  /**
   * 查询bi_manager_member_relation表
   */
  def select_bi_manager_member_relation(): String ={
    val sql =
      """
        | (select  序号   ,
        | province   ,
        | screen_name   ,
        | m_phone   ,
        | user_type   ,
        | project_name   ,
        | 是否已交楼   ,
        | 员工单位   ,
        | 拓客员归属项目   ,
        | member_level   ,
        | manager_idcard   ,
        | manager_sap   ,
        | data   ,
        | manager_phone   ,
        | employee_time   ,
        | date   ,
        | remark  from  bi_manager_member_relation) sparksql_bi_manager_member_relation
        |""".stripMargin
    sql
  }

  /**
   * 查询locate_info表
   */
  def select_locate_info(): String ={
    val sql =
      """
        |(select row_number() over(order by id) as idNumber,
        |id	,
        |broker_id	,
        |device_info	,
        |device_type	,
        |longitude	,
        |latitude	,
        |country	,
        |province	,
        |city	,
        |address	,
        |create_time from  locate_info) sparKsql_locate_info
        |""".stripMargin
    sql
  }

  /**
   * 查询broker_approve表
   */
  def select_broker_approve(): String ={
    val sql = """
         | (select row_number() over(order by id) as idNumber ,
         | id   ,
         | broker_id   ,
         | name   ,
         | gender   ,
         | bank_no   ,
         | bank_name   ,
         | idcard_img   ,
         | idcard_neg_img   ,
         | photo   ,
         | update_time   ,
         | approve_state   ,
         | unapprove_reason   ,
         | idcard_num   ,
         | org_full_name   ,
         | business_license   ,
         | org_code   ,
         | legal_name   ,
         | legal_gender   ,
         | legal_birth   ,
         | legal_idcard   ,
         | agent_name   ,
         | agent_gender   ,
         | agent_birth   ,
         | agent_idcard   ,
         | company_bank_no   ,
         | company_logo   ,
         | photos   ,
         | legal_idcard_img   ,
         | legal_idcard_neg_img   ,
         | agent_idcard_img   ,
         | agent_idcard_neg_img   ,
         | agent_book   ,
         | is_disabled   ,
         | phone   ,
         | apply_time   ,
         | approve_time   ,
         | approve_userid   ,
         | e_mail   ,
         | m_qq   ,
         | weiChat   ,
         | other_reason   ,
         | zhi_fu_bao   ,
         | pay_type   ,
         | user_type   ,
         | ems   ,
         | bank_province   ,
         | id_type   ,
         | attribution_project   ,
         | CONVERT(BIGINT,time_stamp) as time_stamp_new,
         | owner_flag   ,
         | IDCard_start_time   ,
         | IDCard_end_time   ,
         | photo_type   ,
         | is_employee_et   ,
         | sn_city_id   ,
         | sn_employee_code from broker_approve) sparksql_broker_approve
         |""".stripMargin
    sql
  }

  /**
   * 查询bi_kh_manager表,
   */
  def select_bi_kh_manager(): String ={
    val sql =
      """
        |(select 地区公司,
        |会员经理所属部门,
        |会员经理姓名,
        |会员经理手机号码,
        |会员经理身份证号,
        |会员经理发展的会员姓名,
        |会员经理发展的会员手机号码,
        |是否新发展,
        |remark,
        |会员broker_id,
        |会员经理broker_id,
        |memtype from bi_kh_manager) sparksql_bi_kh_manager
        |""".stripMargin
    sql
  }

  /**
   * 查询bi_kh_employee表,
   */
  def select_bi_kh_employee(): String ={
    val sql =
      """
        |( select sap ,
        |姓名   ,
        |地区公司 ,
        |会员经理所属部门 ,
        |会员经理身份证号
        |from  bi_kh_employee ) sparksql_bi_kh_employee
        |""".stripMargin
    sql
  }



}