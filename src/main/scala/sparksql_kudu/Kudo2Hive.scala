package sparksql_kudu

import org.apache.spark.sql.SparkSession

/**
  * Created by 0619653X0 on 2020/8/17.
  */
object Kudo2Hive {
    def main(args: Array[String]): Unit = {
        val kudutable=args(0)
        val hiveTable =args(1)
        val appName=args(2)
        val sparkSession:SparkSession=spark_do_kudu.createSparkSession(appName)
        sparkSession.sql("use hdb")
       // val kudutable="dw_ods.kudu_broker"
        //val hiveTable="ods_hdb_kudu_broker"
        val kuduMasters = "bigdata-prd-cdh-zk-01:7051,bigdata-prd-cdh-nn-02:7051,bigdata-prd-cdh-nn-01:7051"
        val brokerDF = sparkSession.read
            .option("kudu.table", kudutable)
            .option("kudu.master", kuduMasters)
            //200m
            .option("kudu.batchSize", "419430400")
            //10G
            //.option("kudu.splitSizeBytes","10737418240")
            .format("kudu")
            .load()

       // brokerDF.write.saveAsTable("hdb.wcf")
        if("dw_ods.kudu_broker".equals(kudutable) && "ods_hdb_kudu_broker".equals(hiveTable)){
            brokerDF.repartition(100).createOrReplaceTempView("borker")
            sparkSession.sql("insert overwrite table "+hiveTable+" select  guid,r_copy_id,id,m_phone_rsa,m_phone_md5,m_weixin_rsa,m_weixin_md5," +
                "m_qq_rsa,m_qq_md5,account_type,superior_id,broker_type,city,is_disabled int,ios_device,android_device,screen_name_rsa," +
                "screen_name_md5,password_rsa,password_md5,org_account,create_time,build_id,dormant_status,vocation,active_time," +
                "is_employee,login_time,source_from,recommender_id,province_id,is_frozen,frozen_start_time,frozen_end_time,register_from," +
                "is_illegal,is_cheatbroker,register_ip_rsa,register_ip_md5,mini_openId,org_id,org_name,plaintext_pwd_rsa,plaintext_pwd_md5," +
                "org_black_list_flag,disabled_by,union_id,pwd_update_time,gs_country,gs_province,gs_city,gs_area,gs_address_rsa," +
                "gs_address_md5,jpush_id,lbs_city_code,site_city_code,sync_time,update_time,user_group_type from borker")
        } else if("dw_ods.kudu_broker_approve".equals(kudutable) && "ods_hdb_kudu_broker_approve".equals(hiveTable)){
            brokerDF.repartition(100).createOrReplaceTempView("broker_approve")
            sparkSession.sql(
                s"""
                  |insert overwrite table ${hiveTable} select id,
                  |broker_id,
                  |name_rsa,
                  |name_md5,
                  |gender,
                  |bank_no_rsa,
                  |bank_no_md5,
                  |bank_name,
                  |idcard_img,
                  |idcard_neg_img,
                  |photo,
                  |update_time,
                  |approve_state,
                  |unapprove_reason,
                  |idcard_num_rsa,
                  |idcard_num_md5,
                  |idcard_length,
                  |idcard_area,
                  |idcard_birthday,
                  |idcard_sex,
                  |org_full_name,
                  |business_license,
                  |org_code,
                  |legal_name_rsa,
                  |legal_name_md5,
                  |legal_gender,
                  |legal_birth,
                  |legal_idcard_rsa,
                  |legal_idcard_md5,
                  |agent_name_rsa,
                  |agent_name_md5,
                  |agent_gender,
                  |agent_birth,
                  |agent_idcard_rsa,
                  |agent_idcard_md5,
                  |company_bank_no_rsa,
                  |company_bank_no_md5,
                  |company_logo,
                  |photos,
                  |legal_idcard_img,
                  |legal_idcard_neg_img,
                  |agent_idcard_img,
                  |agent_idcard_neg_img,
                  |agent_book,
                  |is_disabled,
                  |phone_rsa,
                  |phone_md5,
                  |apply_time,
                  |approve_time,
                  |approve_userid,
                  |e_mail_rsa,
                  |e_mail_md5,
                  |m_qq_rsa,
                  |m_qq_md5,
                  |weiChat,
                  |other_reason,
                  |zhi_fu_bao_rsa,
                  |zhi_fu_bao_md5,
                  |pay_type,
                  |user_type,
                  |ems,
                  |bank_province,
                  |id_type,
                  |attribution_project,
                  |is_cheatbroker,
                  |owner_flag,
                  |IDCard_start_time,
                  |IDCard_end_time,
                  |photo_type,
                  |is_employee_et,
                  |sn_city_id,
                  |sn_employee_code,
                  |guid,
                  |sync_time,
                  |approve_state_old,
                  |IDCard_start_time_old,
                  |IDCard_end_time_old,
                  |is_cheatbroker_old,
                  |is_employee_et_old,
                  |owner_flag_old,
                  |pay_type_old,
                  |photo_type_old,
                  |update_time_old from broker_approve
                """.stripMargin)
        }else if("dw_ods.kudu_client".equals(kudutable) && "ods_hdb_kudu_client".equals(hiveTable)){
            brokerDF.repartition(100).createOrReplaceTempView("client")
            sparkSession.sql(
               s"""
                  |insert overwrite table ${hiveTable}  select id,
                  |name_rsa,
                  |name_md5,
                  |phone_rsa,
                  |phone_md5,
                  |city,
                  |id_card_rsa,
                  |id_card_md5,
                  |r_build_id from client
                """.stripMargin)
        }else if("dw_ods.kudu_client_building_relation".equals(kudutable) && "ods_hdb_kudu_client_building_relation".equals(hiveTable)){
            brokerDF.repartition(100).createOrReplaceTempView("client_building_relation")
            sparkSession.sql(
                s"""
                  |insert overwrite table ${hiveTable}  select r_build_id,
                  |id,
                  |broker_id,
                  |client_id,
                  |building_id,
                  |client_status,
                  |client_status_old,
                  |is_locked,
                  |locked_at,
                  |unlocked_at,
                  |is_visited,
                  |is_signed,
                  |protect_time,
                  |protect_time_old,
                  |creater,
                  |create_time,
                  |create_time_old,
                  |remark,
                  |city_id,
                  |is_remind,
                  |mingyuan_status,
                  |exhibition_room_id,
                  |client_building_id,
                  |visite_company,
                  |visite_company_id,
                  |visite_place,
                  |visite_place_id,
                  |client_name_rsa,
                  |client_name_md5,
                  |is_id_card,
                  |client_id_card,
                  |activate_sign,
                  |activate_sign_old,
                  |bespeak_time,
                  |source_id,
                  |recommend_source,
                  |platform,
                  |health_intention,
                  |employee_mark,
                  |is_black,
                  |org_id,
                  |org_name,
                  |user_type,
                  |parent_user_type,
                  |update_time,
                  |update_time_old,
                  |is_special,
                  |owner_flag,
                  |is_h5_delete,
                  |client_phone_rsa,
                  |client_phone_md5,
                  |is_tuoke,
                  |is_employee_et,
                  |is_employee_et_old,
                  |property_type,
                  |is_make_up,
                  |is_signing,
                  |member_level,
                  |member_level_old,
                  |member_level_reward,
                  |member_level_reward_old,
                  |my_project_guid,
                  |guid,
                  |client_rb_id,
                  |building_rb_id,
                  |org_rb_Id,
                  |visite_place_rb_id,
                  |visite_company_rb_id,
                  |sync_time,
                  |recommend_ip,
                  |source_rb_id,
                  |province_name,
                  |building_name,
                  |company_id from client_building_relation
                """.stripMargin)
        }else if("dw_ods.kudu_client_building_visited".equals(kudutable) && "ods_hdb_kudu_client_building_visited".equals(hiveTable)){
            brokerDF.repartition(100).createOrReplaceTempView("client_building_visited")
            sparkSession.sql(
                s"""
                  |insert overwrite table ${hiveTable} select
                  |r_build_id,
                  |id,
                  |client_id,
                  |building_id,
                  |status,
                  |status_old,
                  |signSeeHouse,
                  |visited_time,
                  |visited_time_old,
                  |reception,
                  |creater,
                  |remark,
                  |lifecycle_id,
                  |recheck_user,
                  |recheck_mark,
                  |visit_review_status,
                  |purpose,
                  |purpose_old,
                  |withSeeHouse,
                  |my_visited_guid,
                  |my_project_guid,
                  |my_mobile_rsa,
                  |my_mobile_md5,
                  |is_exchange,
                  |is_local_visit,
                  |create_time,
                  |create_time_old,
                  |update_time,
                  |is_lead,
                  |saler_name_rsa,
                  |saler_name_md5,
                  |equipment_id,
                  |broker_id,
                  |client_rb_id,
                  |lifecycle_rb_id,
                  |guid,
                  |building_rb_id,
                  |sync_time,
                  |building_name from client_building_visited
                """.stripMargin)
        }

    }

}
