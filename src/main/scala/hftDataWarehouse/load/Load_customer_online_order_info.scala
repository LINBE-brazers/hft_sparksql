package hftDataWarehouse.load

import hftDataWarehouse.utils.{CreateHFTHiveTableInfo, CreateHiveDatabaseInfoUtils, CreateSparkSessionUtils, SelectRelectionDBTableUtils}
import org.apache.spark.sql.SparkSession

/**
 * 同步customer_online_order_info
 */
object Load_customer_online_order_info {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark: SparkSession = CreateSparkSessionUtils.createSparkSession()
    //创建hive表
    val hivedatabase = args(0)
    val isDropTable = args(1)
    CreateHiveDatabaseInfoUtils.create_database_info(spark,hivedatabase)
    CreateHFTHiveTableInfo.create_table_customer_online_order_info(spark,hivedatabase,isDropTable)

    //同步表相关信息定义
    val real_tableName = "customer_online_order_info"
    val dataFrame_temp_view = "customer_online_order_info_view"
    val customize_tableName = SelectRelectionDBTableUtils.select_customer_online_order_info()

    val task_name = "Load_customer_online_order_info"
    val dbsource_type = "sqlserver"
    CreateHiveDatabaseInfoUtils.load_relationDbTable_to_hiveTable(dbsource_type,task_name,spark,hivedatabase,real_tableName,customize_tableName,dataFrame_temp_view,true)

  }

}
