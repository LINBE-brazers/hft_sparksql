package Extract_Transform_Load

import com.alibaba.fastjson.JSON


object Load_data_from_db_to_bigdata {

  def main(args: Array[String]): Unit = {


    //创建SparkSession
    val spark = CreateSparkSessionUtils.createSparkSession()
    //创建hive表
    val jsonString = args(0)
    println(jsonString.toString)
    val class_name = Class.forName("Extract_Transform_Load.ParameterDTO")
    val json_dto = JSON.parseObject(jsonString,class_name)
    val parameterDTO: ParameterDTO = json_dto.asInstanceOf[ParameterDTO]

    //与hive有关参数
    val hivedatabase = parameterDTO.getHivedatabase()
    val hiveTableName = parameterDTO.getHiveTableName()
    val isNeedPartition = parameterDTO.getIsNeedPartition()
    val isDropTable = parameterDTO.getIsDropTable()

    //创建hive数据库
    CreateHiveDatabaseInfoUtils.create_database_info(spark,hivedatabase)

    //与关系型数据库有关的参数
    val dbsource_type = parameterDTO.getDbsource_type()
    val db_name = parameterDTO.getDb_name()
    val db_ip = parameterDTO.getDb_ip()
    val db_port =  parameterDTO.getDb_port()
    val user_name = parameterDTO.getUser_name()
    val user_password = parameterDTO.getUser_password()
    val real_tableName = parameterDTO.getReal_tableName()

    //获取创建表的的语句和导入表的select语句
    val sparkSqlShell = new SparkSqlShell()
    val returnMap = sparkSqlShell.initSparkSqlShellFromXml(real_tableName, parameterDTO.getDb_name)
    val customize_tableName = returnMap.get("select").asInstanceOf[String]
    val hive_sql = returnMap.get("create").asInstanceOf[String]
    //通过公用方法创建hive表
    CreateHiveDatabaseInfoUtils.create_table_common_center(spark,hivedatabase,hiveTableName,hive_sql,isDropTable)
    //同步表相关信息定义

    val dataFrame_temp_view = hiveTableName+"_view"
    val task_name = "Load_"+hiveTableName
    //开始同步数据
    CreateHiveDatabaseInfoUtils.load_relationDbTable_to_hiveTable(
                                                          dbsource_type,task_name,
                                                          spark,hivedatabase,
                                                          real_tableName,
                                                          hiveTableName,
                                                          customize_tableName,
                                                          dataFrame_temp_view,
                                                          isNeedPartition,db_ip,
                                                          db_port,db_name,
                                                          user_name,user_password
                                                      )
  }

}
