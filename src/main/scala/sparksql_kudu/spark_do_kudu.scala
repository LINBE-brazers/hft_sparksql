package sparksql_kudu

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}


object spark_do_kudu {

  def main(args: Array[String]): Unit = {

    // 1. SparkSession
    val spark: SparkSession = createSparkSession("sparkto_kudu")

    val kuduMasters = "bigdata-dev-nn-002:7051,bigdata-dev-nn-001:7051,bigdata-dev-zk-001:7051"

    val kudu_Context: KuduContext = new KuduContext(kuduMasters,spark.sparkContext)


    val table_name = "spark_do_kudu.sparkDoKuduTable"

//    val studentsDF = spark.read.option("kudu.table", table_name).
//      option("kudu.master", kuduMasters).format("kudu").load()
//    studentsDF.show()

    import spark.implicits._
    val df = Seq(
          Student_Info(48000, "王荣1","F", 19, 164.4f, 116.5f),
          Student_Info(49000, "李晓1","F", 18, 174.4f, 126.5f),
          Student_Info(40000, "李晓1","F", 18, 174.4f, 126.5f),
          Student_Info(41000, "李晓1","F", 18, 174.4f, 126.5f),
          Student_Info(51000, "韦勇1","F", 18, 174.4f, 126.5f),
          Student_Info(61000, "罗茂1","F", 18, 174.4f, 126.5f),
          Student_Info(480000, "王荣1","F", 19, 164.4f, 116.5f),
          Student_Info(490000, "李晓1","F", 18, 174.4f, 126.5f),
          Student_Info(400000, "李晓1","F", 18, 174.4f, 126.5f),
          Student_Info(410000, "李晓1","F", 18, 174.4f, 126.5f),
          Student_Info(510000, "韦勇1","F", 18, 174.4f, 126.5f),
          Student_Info(610000, "罗茂1","F", 18, 174.4f, 126.5f)
    ).toDF()
/*    insert_into_kudu_table(kudu_Context,df,table_name);
    Thread.sleep(100000000)*/



    val structField = Array(
        StructField("sid",IntegerType,nullable = false),
        StructField("name",StringType,nullable = false),
        StructField("gender",StringType,nullable = false),
        StructField("age",IntegerType,nullable = false),
        StructField("height",FloatType,nullable = false),
        StructField("weight",FloatType,nullable = false)
    )

    val schema = StructType(structField)

    //4.2 定义主键(rowkey)
    val keys = Seq("sid")

    import scala.collection.JavaConverters._

    val numBuckets = 6
    val options = new CreateTableOptions().addHashPartitions(List("sid").asJava,numBuckets)
      .setNumReplicas(1)

      if (!kudu_Context.tableExists(table_name)) {
          //kudu_Context.deleteTable(table_name)
          kudu_Context.createTable(tableName = table_name,schema = schema,keys = keys,options = options)
      }
    insert_into_kudu_table(kudu_Context,df, table_name)
    spark.close()
  }

  def insert_into_kudu_table(kudu_Context: KuduContext,df:DataFrame,tableName:String): Unit ={
//    kudu_Context.insertRows(df,tableName)
    kudu_Context.upsertRows(df,tableName)
  }




  /**
   * 创建sparkSession
   * @return
   */
  def createSparkSession(appName: String): SparkSession ={
    val conf = create_spark_conf(appName)
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
  def create_spark_conf(appName: String):SparkConf={
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]").setAppName(appName)
/*      .set("spark.worker.timeout", "500")
      .set("spark.cores.max", "10")
      .set("spark.rpc.askTimeout", "600s")
      .set("spark.network.timeout", "600s")
      .set("spark.task.maxFailures", "1")*/
      .set("spark.speculationfalse", "false")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf
  }

}
