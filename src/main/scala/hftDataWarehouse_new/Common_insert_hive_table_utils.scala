package hftDataWarehouse_new

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object Common_insert_hive_table_utils {

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
    val insert_sql = "insert into table "+hiveDatabase+".ods_"+hiveTableName+" select * from global_temp."+tableNameView
    spark.sql(insert_sql)
    spark.sqlContext.dropTempTable("global_temp."+tableNameView)

  }




}
