import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CovidData {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    val schema = new StructType()
      .add(StructField("regiao", StringType))
      .add(StructField("estado", StringType))
      .add(StructField("municipio", StringType))
      .add(StructField("coduf", IntegerType))
      .add(StructField("codmun", IntegerType))
      .add(StructField("codRegiaoSaude", IntegerType))
      .add(StructField("nomeRegiaoSaude", StringType))
      .add(StructField("data", StringType))
      .add(StructField("semanaEpi", IntegerType))
      .add(StructField("populacaoTCU2019", IntegerType))
      .add(StructField("casosAcumulado", IntegerType))
      .add(StructField("casosNovos", IntegerType))
      .add(StructField("obitosAcumulado", IntegerType))
      .add(StructField("obitosNovos", IntegerType))
      .add(StructField("recuperadosNovos", IntegerType))
      .add(StructField("emAcompanhamentoNovos", IntegerType))
      .add(StructField("interior/metropolitana", IntegerType))

    val covidData = spark.read.schema(schema).option("sep", ";").option("header", "true").csv("/user/data/*csv")
    covidData.createOrReplaceTempView("covid_data")

    val recuperados = spark.sql("select " +
      "recuperadosNovos as CasosRecuperados, " +
      "emAcompanhamentoNovos as EmAcompanhamento " +
      "from covid_data " +
      "where regiao='Brasil' " +
      "order by data desc " +
      "limit 1")

    recuperados.createOrReplaceTempView("tempRecuperados")
    spark.sql("drop table if exists recuperados")
    spark.sql("create table recuperados as select * from tempRecuperados")

    val casos = spark.sql("""
      select casosAcumulado as Acumulado,
      casosNovos as Novos,
      casosAcumulado/(populacaoTCU2019/100000) as Incidencia
      from covid_data
      where regiao='Brasil'
      order by data desc
      limit 1
    """)
      .withColumn("Incidencia", format_number(col("Incidencia"), 1))

    casos.write.format("parquet").mode(SaveMode.Overwrite).save("/user/covid_data/casos")

    val obitos = spark.sql("""
      select obitosAcumulado as Obitos,
      obitosNovos as ObitosNovos,
      obitosAcumulado/(populacaoTCU2019/100000) as Mortalidade,
      obitosAcumulado/casosAcumulado*100 as Letalidade
      from covid_data
      where regiao='Brasil'
      order by data desc
      limit 1
    """)
      .withColumn("Mortalidade", format_number(col("Mortalidade"), 1).cast(FloatType))
      .withColumn("Letalidade", format_number(col("Letalidade"), 2).cast(FloatType))

    val obitosKafka = obitos.withColumn("value", to_json(struct("*")))

    obitosKafka.write.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("topic", "obitos_covid")
      .save()


    val resumo_estado = spark.sql("""
      select regiao, estado,
      casosAcumulado as casos,
      obitosAcumulado as obitos,
      casosAcumulado/(populacaoTCU2019/100000) as Incidencia,
      obitosAcumulado/(populacaoTCU2019/100000) as Mortalidade,
      data
      from covid_data
      where estado is not null and codmun is null and data = '2021-07-06'
      order by regiao asc
    """)
      .withColumn("Mortalidade", format_number(col("Mortalidade"), 1))
      .withColumn("Incidencia", format_number(col("Incidencia"), 1))

    val resumo_regiao = spark.sql("""
      select regiao, null as estado,
      sum(casosAcumulado) as casos,
      sum(obitosAcumulado) as obitos,
      sum(casosAcumulado)/sum((populacaoTCU2019/100000)) as Incidencia,
      sum(obitosAcumulado)/sum((populacaoTCU2019/100000)) as Mortalidade,
      first(data) as data
      from covid_data
      where codmun is null and data = '2021-07-06'
      group by regiao
      order by regiao asc
    """)
      .withColumn("Mortalidade", format_number(col("Mortalidade"), 1))
      .withColumn("Incidencia", format_number(col("Incidencia"), 1))

    val resumo_final = resumo_regiao.union(resumo_estado)

    resumo_final.show(50)

    val esURL = "http://elasticsearch:9200"

    obitos.write
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes.wan.only","true")
      .option("es.nodes", esURL)
      .mode("Overwrite")
      .save("obitos")

  }
}
