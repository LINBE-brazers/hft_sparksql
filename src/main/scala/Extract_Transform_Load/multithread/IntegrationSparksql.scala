package Extract_Transform_Load.multithread

object IntegrationSparksql {
  def main(args: Array[String]): Unit = {

    val spark = CreateSparkSessionUtilsNew.createSparkSession()

    //customer_online_order_info表
    println("================>hdb.ods_hdb_mysql_customer_online_order_info")
    spark.sql("drop table if exists hdb.ods_hdb_mysql_customer_online_order_info")
    val sql_customer_online_order_info =
      """
        |create table if not exists hdb.ods_hdb_mysql_customer_online_order_info stored as parquet as
        |SELECT
        |	c.*, d.path AS order_pdf_url,
        |	d.img_path AS order_image_url
        |FROM
        |	(
        |		SELECT
        |			a.order_code AS order_num,
        |			a.project_guid AS project_guid,
        |			a.store_name AS project_name,
        |			b.product_cname AS room_name,
        |			a.user_name_rsa AS client_name_rsa,
        |			a.user_name_md5 AS client_name_md5,
        |			a.user_mobile_rsa AS client_phone_rsa,
        |			a.user_mobile_md5 AS client_phone_md5,
        |			a.identity_card_number_rsa AS client_idcard_rsa,
        |			a.identity_card_number_md5 AS client_idcard_md5,
        |			a.good_receiver_address_rsa AS client_address_rsa,
        |			a.good_receiver_address_md5 AS client_address_md5,
        |			b.floor_area AS floor_area,
        |			b.standard_floor_unit_price AS floor_unit_price,
        |			b.decoration_standard AS decoration_standard,
        |			b.default_pay_method AS payment_method,
        |			b.product_item_amount AS turnover_amount,
        |     b.zygu_name_rsa as zygu_name_rsa,
        |     b.zygu_phone_md5 as zygu_phone_md5,
        |			a.order_payment_name AS money_name,
        |			a.order_amount AS money_amount,
        |			a.order_status AS STATUS,
        |			a.create_time AS create_time,
        |			a.update_time AS update_time,
        |			a.is_deleted AS is_delete,
        |			b.house_area AS house_area,
        |			b.house_unit_price AS house_unit_price,
        |			a.order_logistics_time AS order_sign_time,
        |			b.decoration_total_price AS decoration_price,
        |			a.user_id AS customer_id,
        |			a.user_email_rsa AS mail_rsa,
        |			a.user_email_md5 AS mail_md5,
        |			b.room_code AS room_guid,
        |			b.is_release AS is_release,
        |			b.release_type AS release_type,
        |			b.is_special_price AS is_discount,
        |			a.sys_source AS is_from_yunke,
        |			a.push_my_status AS is_sync
        |		FROM
        |			hdb.ods_hdb_mysql_so a
        |		LEFT JOIN hdb.ods_hdb_mysql_so_item b ON a.order_code = b.order_code
        |	) c
        |LEFT JOIN hdb.ods_hdb_mysql_so_attachment d ON c.order_num = d.order_code
        |""".stripMargin

    spark.sql(sql_customer_online_order_info)

    //building表
    println("================>hdb.ods_hdb_mysql_building")
    spark.sql("drop table if exists hdb.ods_hdb_mysql_building")
    spark.sql(
      """
        |create table if not exists hdb.ods_hdb_mysql_building stored as parquet as
        |select
        |a.id,
        |b.build_company_id as company_id,
        |a.title as name,
        |get_json_object(b.build_addr_list, '$.provName') as province_name,
        |get_json_object(b.build_addr_list, '$.provCode') as province_code,
        |get_json_object(b.build_addr_list, '$.cityName') as city_name,
        |get_json_object(b.build_addr_list, '$.cityCode') as city_code,
        |(case when b.build_gps regexp('.*-.*') then split(b.build_gps, '-')[0]
        |	  else b.build_gps
        |end) as address_x,
        |(case when b.build_gps regexp('.*-.*') then split(b.build_gps, '-')[1]
        |	  else b.build_gps
        |end) as address_y,
        |c.is_recommend as is_recommand,
        |c.online_status as is_publish,
        |b.build_estate as property_type,
        |b.build_estate_company as property_company,
        |b.build_year as property_rights,
        |b.build_delivery_date as delivery_time,
        |b.build_fixtures as fitment_level,
        |b.build_first_open_date as sale_start_at,
        |b.build_open_date as opening_date,
        |b.build_plot_rate as plot_ratio,
        |a.create_date as create_time,
        |b.build_mingyuan_no as mingyuan_project_id,
        |c.is_special_aisle as employee_recommendation_only,
        |b.build_addr as address,
        |b.build_households as plan_suite,
        |b.build_avg_price as price,
        |b.build_type as building_type,
        |c.is_new_opening as is_new_opening,
        |b.build_sales_status as status,
        |a.create_by as creater,
        |a.last_update_date as last_update_date,
        |b.build_hft_build_id as hft_id,
        |a.is_deleted as is_deleted,
        |d.name as building_company
        |from
        |	hdb.ods_hdb_mysql_prod_product a
        |left join
        |	(select
        |	prod_id,
        |	val['build_company_id'] as build_company_id,
        |	val['build_addr_list'] as build_addr_list,
        |	val['build_gps'] as build_gps,
        |	val['build_estate'] as build_estate,
        |	val['build_estate_company'] as build_estate_company,
        |	val['build_year'] as build_year,
        |	val['build_delivery_date'] as build_delivery_date,
        |	val['build_fixtures'] as build_fixtures,
        |	val['build_first_open_date'] as build_first_open_date,
        |	val['build_open_date'] as build_open_date,
        |	val['build_plot_rate'] as build_plot_rate,
        |	val['build_mingyuan_no'] as build_mingyuan_no,
        |	val['build_addr'] as build_addr,
        |	val['build_households'] as build_households,
        |	val['build_avg_price'] as build_avg_price,
        |	val['build_type'] as build_type,
        |	val['build_is_new'] as build_is_new,
        |	val['build_sales_status'] as build_sales_status,
        |	val['build_hft_build_id'] as build_hft_build_id
        |from
        |(select
        |prod_id,
        |str_to_map(concat_ws(';',collect_set(concat_ws(':', attr_inner_name, str_value))), ';', ':') val
        |from hdb.ods_hdb_mysql_prod_attr2product
        |where is_deleted = 0
        |group by prod_id) a) b on a.id = b.prod_id
        |left join
        |hdb.ods_hdb_mysql_prod_building_online_config c on a.id = c.prod_id
        |left join hdb.ods_hdb_mysql_company d on a.id = d.id
        |where a.is_deleted <> 1
        |""".stripMargin)


    //customer_online_order_pay_info表
    println("================>hdb.ods_hdb_mysql_customer_online_order_pay_info")
    spark.sql("drop table if exists hdb.ods_hdb_mysql_customer_online_order_pay_info")
    spark.sql(
      """
        |create table if not exists hdb.ods_hdb_mysql_customer_online_order_pay_info stored as parquet as
        |select
        |b.order_code as order_num,
        |a.payee_merchant_num as merchant_num,
        |a.payee_wechat_num_rsa as merchant_wechat_rsa,
        |a.payee_wechat_num_md5 as merchant_wechat_md5,
        |a.payee_bank_name as merchant_bank_name,
        |a.payee_bank_num_rsa as merchant_bank_num_rsa,
        |a.payee_bank_num_md5 as merchant_bank_num_md5,
        |a.order_payment_name as pay_method,
        |a.order_payment_confirm_date as pay_time,
        |b.amount as pay_amout,
        |b.create_time as create_time,
        |b.update_time as update_time,
        |a.payee_name_rsa as zjAccountProjBuName_rsa,
        |a.payee_name_md5 as zjAccountProjBuName_md5,
        |a.payee_address_rsa as zjAccountProjBuAdd_rsa,
        |a.payee_address_md5 as zjAccountProjBuAdd_md5,
        |a.payee_mobile_rsa as zjAccountProjBuPhone_rsa,
        |a.payee_mobile_md5 as zjAccountProjBuPhone_md5,
        |c.sign_id as zjAccountSignatureID,
        |a.order_payment_status as is_pay_success
        |from hdb.ods_hdb_mysql_so a
        |left join hdb.ods_hdb_mysql_so_orderpay_fllow b on a.order_code = b.order_code
        |left join hdb.ods_hdb_mysql_so_attachment c on a.order_code = c.order_code
        |""".stripMargin)


    //hdb.ods_hdb_mysql_bi_kh_employee
    println("================>hdb.ods_hdb_mysql_bi_kh_employee")
//    spark.sql("drop table if exists hdb.ods_hdb_mysql_bi_kh_employee_old")
//    spark.sql("create table hdb.ods_hdb_mysql_bi_kh_employee_old as select * from hdb.ods_hdb_mysql_bi_kh_employee")
    spark.sql(
      """
        |insert overwrite table hdb.ods_hdb_mysql_bi_kh_employee
        |select
        |sap,
        |rsa(`姓名`) as `姓名_rsa`,
        |md5(`姓名`) as `姓名_md5`,
        |`地区公司`,
        |`会员经理所属部门`,
        |rsa(substring(upper(`会员经理身份证号`), 1, 18)) as `会员经理身份证号_rsa`,
        |md5(substring(upper(`会员经理身份证号`), 1, 18)) as `会员经理身份证号_md5`,
        |`岗位`,
        |`是否在职`,
        |`离职时间`,
        |`入职恒大宝时间`
        |from hft.ods_hft_sqlserver_bi_kh_employee
        |""".stripMargin)


    println("================>hdb.ods_hdb_mysql_db_s_owner_my")
    spark.sql(
      """
        |insert overwrite table hdb.ods_hdb_mysql_db_s_owner_my
        |select company_name
        |,project_guid
        |,project_name
        |,room_name
        |,ras(customer_name) as customer_name_rsa
        |,md5(customer_name) as customer_name_md5
        |,rsa(substring(upper(customer_idcard), 1, 18)) as customer_idcard_rsa
        |,md5(substring(upper(customer_idcard), 1, 18)) as customer_idcard_md5
        |,sale_status
        |,first_subscribe_date
        |,subscribe_date
        |,contract_signing_date
        |,tel
        |,recommend_id
        |,deal_amount
        |from hft.ods_hft_sqlserver_db_s_owner_my
        |""".stripMargin)

    spark.stop()
  }
}
