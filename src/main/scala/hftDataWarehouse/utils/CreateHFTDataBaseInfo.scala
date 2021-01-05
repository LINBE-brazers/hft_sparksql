package hftDataWarehouse.utils

/**
 * 创建hive数据库通用类
 */
object CreateHFTDataBaseInfo {


  /**
   * 创建数据库
   * @param args
   */
  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = CreateSparkSessionUtils.createSparkSession()
    //创建hive数据库
    val hivedatabase = "hft_database"
    CreateHiveDatabaseInfoUtils.create_database_info(spark,hivedatabase)

  }

}
