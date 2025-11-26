# pyspark_scripts


## Primeiro script - enviando dados para uma API rest em paralelo e assíncrono no spark

## Segundo script - criando tables usando delta lake

* Para executar o delta lake é necessário baixar as dependências do delta lake como o jar e as bibliotecas do python.

-> https://github.com/delta-io/delta/releases/tag/v0.7.0 - lib python delta lake

-> https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake - doc do delta lake com as versões correspondentes para o spark

-> https://mvnrepository.com/artifact/io.delta/delta-core_2.12/0.7.0 - Maven jar para a versão 0.7.0, compativel com spark 3.0.1


### Start pyspark shell with delta libaries from server
pyspark --packages io.delta:delta-core_2.12:0.7.0 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

### Start pyspark shell with delta libaries from locally
pyspark --jars /path/teste_delta_lake/libs/delta-core_2.12-0.7.0.jar \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        --py-files /path/teste_delta_lake/libs/delta.zip
