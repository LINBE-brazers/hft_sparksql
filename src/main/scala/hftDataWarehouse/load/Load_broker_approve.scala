package hftDataWarehouse.load

import hftDataWarehouse.utils.{CreateHFTHiveTableInfo, CreateHiveDatabaseInfoUtils, CreateSparkSessionUtils, SelectRelectionDBTableUtils}

/**
 * 同步broker_approve表
 */
object Load_broker_approve {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = CreateSparkSessionUtils.createSparkSession()
    //创建hive表
    val hivedatabase = args(0)
    val isDropTable = args(1)
    CreateHiveDatabaseInfoUtils.create_database_info(spark,hivedatabase)
    CreateHFTHiveTableInfo.create_table_broker_approve(spark,hivedatabase,isDropTable)

    //同步表相关信息定义
    val real_tableName = "broker_approve"
    val dataFrame_temp_view = "broker_approve_view"
    val customize_tableName = SelectRelectionDBTableUtils.select_broker_approve()

    val task_name = "Load_broker_approve"
    val dbsource_type = "sqlserver"
    CreateHiveDatabaseInfoUtils.load_relationDbTable_to_hiveTable(dbsource_type,task_name,spark,hivedatabase,real_tableName,customize_tableName,dataFrame_temp_view,true)

  }

}
