package br.com.covid

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._

object ElasticCovidData {

  def apply(spark: SparkSession): Unit = {

    val esURL = "https://imunizacao-es.saude.gov.br/"

    val reader = spark.read
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes.wan.only","true")
      .option("es.port", "443")
      .option("es.net.ssl", "true")
      .option("es.net.http.auth.user","imunizacao_public")
      .option("es.net.http.auth.pass","qlto5t&7r_@+#Tlstigi")
      .option("es.nodes", esURL)

    val data = reader.load("desc-imunizacao/_search")

    data.write.format("csv").option("sep", ";").option("header", "true").save("/user/elastic_data")

  }

}
