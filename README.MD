# Spark Covid

Projeto responsável por ler arquivos no formato csv, contendo dados sobre a Covid-19,
salvar em um cluster hdfs, e gerar visualizações sobre esses dados.

Para a execução do projeto, é necessario estar em um ambiente com HDFS, HIVE, SPARK, KAFKA
e ELASTICSEARCH configurados.

No servidor ou container SPARK é necessário executar o comando:  

**spark-submit --packages org.elasticsearch:elasticsearch-hadoop:<versao-es> spark-covid_2.11-0.1.jar /local/path/to/csv**  

onde <versao-es> deve ser subtituido pela versão do ELASTICSEARCH e /local/path/to/csv deve ser subtituido por um diretório,
no container, que contenha os arquivos csv.

Os arquivos podem ser baixados no link:  
https://mobileapps.saude.gov.br/esus-vepi/files/unAFkcaNDeXajurGB7LChj8SgQYS2ptm/04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar




