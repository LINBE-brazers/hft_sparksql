package Extract_Transform_Load

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CreateSparkSessionUtils {
  /**
   * 创建sparkSession
   * @return
   */
  def createSparkSession(): SparkSession ={
    val conf = create_spark_conf()
    val spark: SparkSession = SparkSession
      //构建SparkSession
      .builder()
      //启用hive支持
      .config(conf)
      .enableHiveSupport()
      //获取或者创建
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  /**
   * 创建spark配置项
   * @return
   */
  def create_spark_conf():SparkConf={
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName("hftDataWarehouse_project")
      .set("spark.sql.warehouse.dir", "/user/hive/warehouse")
      //设置并行度，默认200
      .set("spark.default.parallelism", "10")
      .set("spark.sql.shuffle.partitions", "10")
      //开启shuffle map端生产文件的合并
      .set("spark.shuffle.consolidateFiles", "true")
      //选取序列化机制，使用kyro，丢弃默认java序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //开启针对大查询sql的字节码编译，对于小查询不建议开启
      .set("spark.sql.codegen", "true")
      //开启shuffle数据的压缩
      .set("spark.shuffle.compress", "true")
      //增大shuffle map端缓冲区的大小，默认32k
      .set("spark.shuffle.file.buffer", "256k")
      //开启数据本地化NODE_LOCAL级别等待时间，默认3秒，根据日志增减
      .set("spark.locality.wait.node", "6s")
      //开启数据本地化PROCESS_LOCAL级别等待时间，默认3秒，根据日志增减
      .set("spark.locality.wait.process", "6s")
      //jvm 内存的占比设定，默认0.6
      .set("spark.memory.fraction", "0.3")
      //设置executor的缓存内存的占比
      .set("spark.memory.storageFraction", "0.8")
    conf
  }

}
