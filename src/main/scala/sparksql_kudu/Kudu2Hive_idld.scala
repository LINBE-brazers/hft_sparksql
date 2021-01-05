package sparksql_kudu

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import utils.DateUtils

import scala.collection.mutable

object Kudu2Hive_idld {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = spark_do_kudu.createSparkSession("Kudu2Hive_idld")

    val serviceConfMap = new mutable.HashMap[String, String]()
    //解析参数
    args.foreach(arg => {
      println(arg)
      val argArr: Array[String] = arg.split("=")
      if (argArr.length > 1) {
        serviceConfMap.put(argArr(0), argArr(1))
      }
    })


    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition=true")
    val kuduMasters = "bigdata-prd-cdh-zk-01:7051,bigdata-prd-cdh-nn-02:7051,bigdata-prd-cdh-nn-01:7051"
    var brokerDF = spark.read
      .option("kudu.table", s"${serviceConfMap.getOrElse("kudu_database", "")}.${serviceConfMap.getOrElse("kudu_table", "")}")
      .option("kudu.master", kuduMasters)
      .format("kudu")
      .load()

    //判断是否存在增量字段
    if (serviceConfMap.contains("update_col_name")) {
      brokerDF = brokerDF.filter(row => {
        val str = row.getAs[Timestamp](serviceConfMap.getOrElse("update_col_name", ""))
        if (str == null) {
          false
        } else {
          val time = DateUtils.stampToStr(str)
          time >= serviceConfMap.getOrElse("start_time", "") && time < serviceConfMap.getOrElse("end_time", "")
        }
      })
    }

    brokerDF.persist().createOrReplaceTempView("temp_view_idld")

    //TODO: 测试
    /*
    spark.sql(s"drop table if exists hdb_test.${serviceConfMap.getOrElse("hive_table", "")}_idld")
    spark.sql(s"create table if not exists hdb_test.${serviceConfMap.getOrElse("hive_table", "")}_idld stored as parquet as select * from temp_view_idld")
     */

    println("DataFrame count: " + brokerDF.count())


    //设置临时表名
    val tmpTableName = serviceConfMap.getOrElse("hive_database", "") + "." + serviceConfMap.getOrElse("hive_table", "tmp") + "_idld"
    serviceConfMap.put("idld_table_name", tmpTableName)

    //1、删除临时表
    var sql = s"drop table if exists $tmpTableName"
    invokeSql(spark, sql)

    //2、增量创建临时表
    sql = XMLUtils.selectMapping(
      XMLUtils.getXmlStr(s"/sql/kudu_idld/${serviceConfMap.getOrElse("hive_table", "")}.xml"),
      serviceConfMap)
    //执行sql
    invokeSql(spark, sql)

    //3、删除原表
    val targetTable = serviceConfMap.getOrElse("hive_database", "") + "." + serviceConfMap.getOrElse("hive_table", "")
    sql = s"drop table if exists $targetTable"
    invokeSql(spark, sql)

    //4、临时表重命名为目录表
    invokeSql(spark, s"alter table $tmpTableName rename to $targetTable")

    brokerDF.unpersist()
    spark.stop()
  }


  def invokeSql(spark: SparkSession, sql: String): Unit = {
    println("================================")
    println(sql)
    spark.sql(sql)
  }


}
