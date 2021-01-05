package hftDataWarehouse.load

import hftDataWarehouse.utils.{CreateHFTHiveTableInfo, CreateHiveDatabaseInfoUtils, CreateSparkSessionUtils, SelectRelectionDBTableUtils}

/**
 * 同步client_building_visited表
 */
object Load_client_building_visited {

  def main(args: Array[String]): Unit = {

       //创建SparkSession
    val spark = CreateSparkSessionUtils.createSparkSession()
    //创建hive表
    val hivedatabase = args(0)
    val isDropTable = args(1)
    CreateHiveDatabaseInfoUtils.create_database_info(spark,hivedatabase)
    CreateHFTHiveTableInfo.create_table_client_building_visited(spark,hivedatabase,isDropTable)

    //同步表相关信息定义
    val real_tableName = "client_building_visited"
    val dataFrame_temp_view = "client_building_visited_view"
    val customize_tableName = SelectRelectionDBTableUtils.select_client_building_visited()

    val task_name = "Load_client_building_visited"
    val dbsource_type = "sqlserver"
    CreateHiveDatabaseInfoUtils.load_relationDbTable_to_hiveTable(dbsource_type,task_name,spark,hivedatabase,real_tableName,customize_tableName,dataFrame_temp_view,true)

  }

}
