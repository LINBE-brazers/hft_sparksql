package hftDataWarehouse.load

import hftDataWarehouse.utils.CreateSparkSessionUtils

object Drop_Data_base_info {

  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = CreateSparkSessionUtils.createSparkSession()
    val hivedatabase = args(0)
    spark.sql("drop database "+hivedatabase+" cascade")
  }
}
