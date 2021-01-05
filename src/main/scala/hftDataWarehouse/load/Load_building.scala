package hftDataWarehouse.load

import hftDataWarehouse.utils.{CreateDadtabaseInfoUtils, CreateHFTHiveTableInfo, CreateHiveDatabaseInfoUtils, CreateSparkSessionUtils, SelectRelectionDBTableUtils}
import utils.Config

/**
 * 同步building表
 */
object Load_building {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = CreateSparkSessionUtils.createSparkSession()
    //创建hive表
    val hivedatabase = args(0)
    val isDropTable = args(1)
    CreateHiveDatabaseInfoUtils.create_database_info(spark,hivedatabase)
    CreateHFTHiveTableInfo.create_table_building(spark,hivedatabase,isDropTable)

    //同步表相关信息定义
    val real_tableName = "building"
    val dataFrame_temp_view = "building_view"
    val customize_tableName = SelectRelectionDBTableUtils.select_building()

    val task_name = "Load_building"
    val dbsource_type = "sqlserver"
    CreateHiveDatabaseInfoUtils.load_relationDbTable_to_hiveTable(dbsource_type,task_name,spark,hivedatabase,real_tableName,customize_tableName,dataFrame_temp_view,true)

  }

}
