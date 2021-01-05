package Extract_Transform_Load.multithread

import org.apache.spark.sql.SparkSession

object Load_data_utils {

  /**
   * 同步数据中间方法
   *
   * @param spark
   * @param parameterDTO
   */
  def load_data_commom(spark: SparkSession, parameterDTO: ParameterDTONew): Unit = {

    //与hive有关参数
    val hivedatabase = parameterDTO.getHivedatabase
    val hiveTableName = parameterDTO.getHiveTableName
    val isNeedPartition = parameterDTO.getIsNeedPartition
    val isDropTable = parameterDTO.getIsDropTable
    val truncate_table = parameterDTO.getTruncate_table

    //获取创建表的的语句和导入表的select语句
    val customize_tableName = parameterDTO.getSelect
    val hive_sql = parameterDTO.getCreate

    //创建hive数据库
    CreateHiveDatabaseInfoUtilsNew.create_database_info(spark, hivedatabase)

    //与关系型数据库有关的参数
    val dbsource_type = parameterDTO.getDbsource_type
    val db_name = parameterDTO.getDb_name
    val db_ip = parameterDTO.getDb_ip
    val db_port = parameterDTO.getDb_port
    val user_name = parameterDTO.getUser_name
    val user_password = parameterDTO.getUser_password
    val real_tableName = parameterDTO.getReal_tableName

    //通过公用方法创建hive表
    //    CreateHiveDatabaseInfoUtilsNew.create_table_common_center(spark,hivedatabase,hiveTableName,hive_sql,isDropTable,truncate_table)
    //同步表相关信息定义
    val dataFrame_temp_view = hiveTableName + "_view"
    val task_name = "Load_" + hiveTableName
    //开始同步数据
    CreateHiveDatabaseInfoUtilsNew.load_relationDbTable_to_hiveTable(
      dbsource_type, task_name,
      spark, hivedatabase,
      real_tableName,
      hiveTableName,
      customize_tableName,
      dataFrame_temp_view,
      isNeedPartition, db_ip,
      db_port, db_name,
      user_name, user_password,
      parameterDTO
    )

  }
}
