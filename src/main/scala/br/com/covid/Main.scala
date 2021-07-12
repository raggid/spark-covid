package br.com.covid

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    CovidData(spark, args(0))
//    ElasticCovidData(spark)
  }

}
