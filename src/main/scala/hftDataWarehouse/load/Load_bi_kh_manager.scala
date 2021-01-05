package hftDataWarehouse.load

import hftDataWarehouse.utils.{CreateHFTHiveTableInfo, CreateHiveDatabaseInfoUtils, CreateSparkSessionUtils, SelectRelectionDBTableUtils}

/**
 * 同步bi_kh_manager表
 */
object Load_bi_kh_manager {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = CreateSparkSessionUtils.createSparkSession()
    //创建hive表
    val hivedatabase = args(0)
    val isDropTable = args(1)
    CreateHiveDatabaseInfoUtils.create_database_info(spark,hivedatabase)
    CreateHFTHiveTableInfo.create_table_bi_kh_manager(spark,hivedatabase,isDropTable)

    //同步表相关信息定义
    val real_tableName = "bi_kh_manager"
    val dataFrame_temp_view = "bi_kh_manager_view"
    val customize_tableName = SelectRelectionDBTableUtils.select_bi_kh_manager()

    val task_name = "Load_bi_kh_manager"
    val dbsource_type = "sqlserver"
    CreateHiveDatabaseInfoUtils.load_relationDbTable_to_hiveTable(dbsource_type,task_name,spark,hivedatabase,real_tableName,customize_tableName,dataFrame_temp_view,false)

  }

}
