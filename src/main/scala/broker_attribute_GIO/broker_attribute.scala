package broker_attribute_GIO

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}

object broker_attribute {


  def main(args: Array[String]): Unit = {

    val spark = CreateSparkSessionUtils_broker.createSparkSession()

    val sql =
      """
        |select
        | cast(c.username_ppl as String) as username_ppl,
        | cast(c.username_md5_ppl as String) as username_md5_ppl,
        | cast(c.union_id as String) as union_id,
        | cast(c.phone_ppl as String) as phone_ppl,
        | cast(c.phone_md5_ppl as String) as phone_md5_ppl,
        | cast(c.residence_ppl as String) as residence_ppl,
        | cast(c.regtime_ppl as String) as regtime_ppl,
        | cast(c.gender_ppl as String) as gender_ppl,
        | cast(c.brithday_ppl as String) as brithday_ppl,
        | cast(c.age_ppl as String) as age_ppl,
        | cast(c.ifhengda_ppl as String) as ifhengda_ppl,
        | cast(c.broker_id as String) as broker_id,
        | cast(d.broker_level as String) as viplevel_ppl,
        | cast(c.approve_state as String) as ifattestation_ppl
        | from (select
        |    a.screen_name_rsa as username_ppl,
        |    a.screen_name_md5   as username_md5_ppl,
        |    a.union_id as union_id,
        |    a.m_phone_rsa as phone_ppl,
        |    a.m_phone_md5 as phone_md5_ppl,
        |    a.residence_ppl as residence_ppl,
        |    a.create_time as regtime_ppl,
        |    b.idcard_sex as gender_ppl,
        |    b.idcard_birthday as brithday_ppl,
        |    if(datediff(CURRENT_DATE,CONCAT(substr(CURRENT_DATE,0,4),substr(from_unixtime(unix_timestamp(b.idcard_birthday,'yyyyMMdd'),'yyyy-MM-dd'),5,7)))>=0,
        |	    (substr(CURRENT_DATE,0,4) - substr(from_unixtime(unix_timestamp(b.idcard_birthday,'yyyyMMdd'),'yyyy-MM-dd'),0,4)),
        |	    (substr(CURRENT_DATE,0,4) - substr(from_unixtime(unix_timestamp(b.idcard_birthday,'yyyyMMdd'),'yyyy-MM-dd'),0,4)-1)) as age_ppl,
        |    b.user_type as ifhengda_ppl,
        |    b.broker_id as broker_id,
        |	   nvl(b.approve_state, '0') as approve_state
        |from  (
        |        select aa.*,concat(bb.province_name,cc.city_name,dd.area_name) as residence_ppl  from  hdb.ods_hdb_mysql_broker  aa
        |        left join hdb.ods_hdb_mysql_broker_province  bb on aa.gs_province = bb.province_code
        |        left join hdb.ods_hdb_mysql_broker_city  cc on aa.gs_city = cc.city_code
        |        left join hdb.ods_hdb_mysql_broker_area  dd on aa.gs_area = dd.area_code
        |      ) a
        |       left  join hdb.ods_hdb_mysql_broker_approve b on a.guid = b.guid
        |       where a.union_id is not null)c
        |       left  join hdb.ods_hdb_mysql_broker_member_level d on c.union_id = d.union_id
        |""".stripMargin

    val df = spark.sql(sql)


    import spark.implicits._
    val rdd_from_df: RDD[Row] = df.rdd
    val rdd_end: RDD[String] = rdd_from_df.map(row => {
      val returnStr = create_broker_object_info(row)
      returnStr
    })
    val df_end = rdd_end.toDF("json_string")
    val df_end_repartition = df_end.repartition(40)

    df_end_repartition.write.format("text").mode(SaveMode.Overwrite).save("hdfs://10.71.81.220:8020/ftp/admin/jobs/importer/DI-202007220744-27878933000000")
  }

  /**
   * 创建用户属性
   *
   * @param row
   * @return
   */
  def create_broker_object_info(row: Row): String = {
    val username_ppl = row.getAs[String]("username_ppl")
    val username_md5_ppl = row.getAs[String]("username_md5_ppl")
    val union_id = row.getAs[String]("union_id")
    val phone_ppl = row.getAs[String]("phone_ppl")
    val phone_md5_ppl = row.getAs[String]("phone_md5_ppl")
    val residence_ppl = row.getAs[String]("residence_ppl")
    val regtime_ppl = row.getAs[String]("regtime_ppl")
    val gender_ppl = row.getAs[String]("gender_ppl")
    val brithday_ppl = row.getAs[String]("brithday_ppl")
    val age_ppl = row.getAs[String]("age_ppl")
    val ifhengda_ppl = row.getAs[String]("ifhengda_ppl")
    val broker_id = row.getAs[String]("broker_id")
    val viplevel_ppl = row.getAs[String]("viplevel_ppl")
    val ifattestation_ppl = row.getAs[String]("ifattestation_ppl")
    val broker_info = new broker_attribute_POJO()
    broker_info.setUserId(union_id)
    val attrs = broker_info.getAttrs
    attrs.put("username_ppl", username_ppl)
    attrs.put("username_md5_ppl", username_md5_ppl)
    attrs.put("phone_ppl", phone_ppl)
    attrs.put("phone_md5_ppl", phone_md5_ppl)
    attrs.put("residence_ppl", residence_ppl)
    attrs.put("regtime_ppl", regtime_ppl)
    attrs.put("gender_ppl", gender_ppl)
    attrs.put("brithday_ppl", brithday_ppl)
    attrs.put("age_ppl", age_ppl)
    attrs.put("ifhengda_ppl", ifhengda_ppl)
    attrs.put("broker_id", broker_id)
    attrs.put("viplevel_ppl", viplevel_ppl)
    attrs.put("ifattestation_ppl", ifattestation_ppl)
    broker_info.toString
  }

}
