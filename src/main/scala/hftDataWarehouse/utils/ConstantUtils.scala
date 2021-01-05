package hftDataWarehouse.utils

/***
 * 常量的定义工具类
 */
object ConstantUtils {


    //关系型数据库的类型
    val DB_SQL_SERVER_TYPE = "sqlserver"
    val DB_SQL_SERVER_1_TYPE = "sqlserver_1"
    val DB_MYSQL_TYPE = "mysql"


    //hive的字段类型
    val TYPE_INT = "int"
    val TYPE_BIGINT = "bigint"
    val TYPE_FLOAT = "float"
    val TYPE_DOUBLE = "double"
    val TYPE_STRING = "String"

    //删除表（drop table）开关控制字段
    val IS_DROP_TABLE = "Y"

    //目标库的类型
    val TARGET_TYPE_HIVE = "hive"
    val TARGET_TYPE_KUDU = "kudu"

}
