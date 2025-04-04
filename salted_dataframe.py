import pyspark.sql.functions as f
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("teste").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
   
base_pos = (
        spark.read.format('bigquery')
        .option('maxParallelism', 10)
        .option('preferredMinParallelism', 10)
        .option('table',f'project_id:dataset.table_base')
        .option('filter', "partition = 'xpto'").load().select("id", "telefone", "id_plano")
)

base_pos.createOrReplaceTempView("base_pos")

base_pos =  spark.sql("""
    select concat(cast(id_plano as integer), '_', FLOOR(RAND(123456) * 19)) as salted_key, id, telefone
    from base_pos
""")

base_pos.createOrReplaceTempView("base_skewed")

plano_tarifario = spark.read.format("bigquery") \
    .load("project_id.dataset.table_plano").select("id_plano", "dsc_plano", "nivel_plano")

plano_tarifario.createOrReplaceTempView("plano")

plano_tarifario = spark.sql("""
    select id_plano, dsc_plano, nivel_plano, explode(array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)) as salted_key
    from plano
""")
plano_tarifario.createOrReplaceTempView("plano_exploded")

plano_tarifario = spark.sql("""
    select id_plano, dsc_plano, nivel_plano, concat(id_plano, '_', salted_key) as salted_key
    from plano_exploded
""")

plano_tarifario.createOrReplaceTempView("plano_salted")

df_final = spark.sql("""
    select *
    from base_skewed fact
    left join plano_salted dim
    on fact.salted_key = dim.salted_key
""")

print(df_final.count())
