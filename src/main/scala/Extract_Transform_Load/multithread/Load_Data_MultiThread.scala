package Extract_Transform_Load.multithread

import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList, Executors}

import Extract_Transform_Load.{ETLUtils, SparkSqlShell}
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession
import utils.{Config, RSAUtils}

import scala.collection.{JavaConversions, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}


object Load_Data_MultiThread {



  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = CreateSparkSessionUtilsNew.createSparkSession()

    //创建hive表
    val jsonString = args(0)
    val class_name = Class.forName("Extract_Transform_Load.multithread.ParameterDTONew")
    val json_dto = JSON.parseObject(jsonString, class_name)
    val parameterDTONew: ParameterDTONew = json_dto.asInstanceOf[ParameterDTONew]
    val sparkSqlShell = new SparkSqlShell()
    val real_tableName = parameterDTONew.getReal_tableName
    val db_name = parameterDTONew.getDb_name

    //获取同步数据的create语句和select语句
    val returnMap = sparkSqlShell.initSparkSqlShellFromXml(real_tableName, parameterDTONew.getDb_name)
    parameterDTONew.setCreate(returnMap.get("create").asInstanceOf[String])
    parameterDTONew.setSelect(returnMap.get("select").asInstanceOf[String])
    if (returnMap.containsKey("rsa_columns")) {
      parameterDTONew.setRsa_columns(returnMap.get("rsa_columns").asInstanceOf[String])
    } else {
      parameterDTONew.setRsa_columns("")
    }
    if (returnMap.containsKey("dataframe_columns")) {
      parameterDTONew.setDataframe_columns(returnMap.get("dataframe_columns").asInstanceOf[String])
    } else {
      parameterDTONew.setDataframe_columns("")
    }

    if (returnMap.containsKey("idld_sql")) {
      parameterDTONew.setIdld_sql(returnMap.get("idld_sql").asInstanceOf[String])
    } else {
      parameterDTONew.setIdld_sql("")
    }
    //获取配置的IP规则：10.101.40.96|2|4,10.101.40.99|1|3
    //IP,库名开始编号和最终编号
    val db_ip = parameterDTONew.getDb_ip();

    //创建表
    CreateHiveDatabaseInfoUtilsNew.create_table_common_center(spark, parameterDTONew)
    parameterDTONew.setIsDropTable("N")
    parameterDTONew.setTruncate_table("N")

    //同步数据判断
    println("=====================" + db_ip.contains("|"))
    if (!db_ip.contains("|")) { //同步单库单表情况
      println("IP========" + db_ip + ";==============" + db_name + ";Real_tableName==" + parameterDTONew.getReal_tableName)
      Load_data_utils.load_data_commom(spark, parameterDTONew)
    } else { //同步分表分库情况
      val db_ip_array = db_ip.split(",")
      //dbname -> ip
      val ints = new ListBuffer[String]()

      //遍历数组，以IP为基础，逐个同步对应IP下的数据库对应的表
      for (ip_number <- db_ip_array) {
        val ip_number_array = ip_number.split("\\|")
        val ip = ip_number_array(0) //数据库IP
        val start_offset = ip_number_array(1) //数据库名开始编号
        val end_offset = ip_number_array(2) //数据库名结束编号
        val end_offset_new = Integer.parseInt(end_offset)
        var flag = Integer.parseInt(start_offset)
        while (flag <= end_offset_new) {
          ints += ip + "," + db_name + "_" + flag
          flag += 2
        }
      }

      //打乱数组顺序
      for (i <- 0 until(ints.size)) {
        val rand: Int = (Math.random() * (ints.size - 1)).toInt
        val current = ints(i)
        ints(i) = ints(rand)
        ints(rand) = current
      }

      //多线程同步
      val pool = Executors.newFixedThreadPool(8)
//      val pool = Executors.newFixedThreadPool(ints.length)
      implicit val ec = ExecutionContext.fromExecutorService(pool)
      try {
        val futures = ints.toVector.map(flag => Future {
          val param: ParameterDTONew = parameterDTONew.clone().asInstanceOf[ParameterDTONew]
          val ipDbs = flag.split(",")
          param.setDb_ip(ipDbs(0))
          param.setDb_name(ipDbs(1))
          println("IP========" + ipDbs(0) + ";==============" + ipDbs(1) + ";Real_tableName==" + param.getReal_tableName)
          Load_data_utils.load_data_commom(spark, param)
        })
        futures.foreach(future => Await.result(future, Duration.Inf))
      } catch {
        case e: Exception => throw new Exception(e)
      } finally {
        ec.shutdown()
      }
    }
    //执行增量同步sql
//    idldSqlInvoke(spark, parameterDTONew)
  }

  def idldSqlInvoke(spark: SparkSession, parameterDTONew: ParameterDTONew): Unit = {
    if (parameterDTONew.getIdld_sql == null || parameterDTONew.getIdld_sql.isEmpty) return
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val sqlStr = ETLUtils.selectMapping(parameterDTONew.getIdld_sql, parameterDTONew)
    sqlStr.split(";").foreach(sql => {
      if (sql.trim.length > 0) {
        println(sql)
        spark.sql(sql)
      }
    })

    //TODO:测试
    val sql_test =
      s"""
        |select mysql_dbname,count(1) from ${parameterDTONew.getHivedatabase}.${parameterDTONew.getHiveTableName.replace("idld","ods")} group by mysql_dbname""".stripMargin
    spark.sql(sql_test).show(100)
  }

}


