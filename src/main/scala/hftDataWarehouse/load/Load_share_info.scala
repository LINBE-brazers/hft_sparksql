package hftDataWarehouse.load

import hftDataWarehouse.utils.{CreateHFTHiveTableInfo, CreateHiveDatabaseInfoUtils, CreateSparkSessionUtils, SelectRelectionDBTableUtils}

/**
 * 同步share_info表
 */
object Load_share_info {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = CreateSparkSessionUtils.createSparkSession()
    //创建hive表
    val hivedatabase = args(0)
    val isDropTable = args(1)
    CreateHiveDatabaseInfoUtils.create_database_info(spark,hivedatabase)
    CreateHFTHiveTableInfo.create_table_share_info(spark,hivedatabase,isDropTable)

    //同步表相关信息定义
    val real_tableName = "share_info"
    val dataFrame_temp_view = "share_info_view"
    val customize_tableName = SelectRelectionDBTableUtils.select_share_info()

    val task_name = "Load_share_info"
    val dbsource_type = "sqlserver_1"
    CreateHiveDatabaseInfoUtils.load_relationDbTable_to_hiveTable(dbsource_type,task_name,spark,hivedatabase,real_tableName,customize_tableName,dataFrame_temp_view,true)

  }

}
