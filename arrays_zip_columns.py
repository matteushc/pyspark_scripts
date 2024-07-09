from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
from pyspark.sql.functions import array, col, struct, explode, collect_list, concat_ws, arrays_zip
from pyspark.sql.types import StringType, LongType

spark = SparkSession.builder.getOrCreate()


df = spark.createDataFrame([
    Row(cpf='11111122222', nome='Pedro' ,ds_contato_1='pedro@gmail.com', ds_contato_2='12@timbrasil.com.br', ds_contato_3='13@timbrasil.com.br', ds_address_1="tim", ds_address_2="gmail",ds_address_3="",cep_1="",cep_2="",cep_3=""),
    Row(cpf='11111122223', nome='Carlos' ,ds_contato_1='carlos@gmail.com', ds_contato_2='22@timbrasil.com.br', ds_contato_3='23@timbrasil.com.br', ds_address_1="tim", ds_address_2="gmail",ds_address_3="",cep_1="",cep_2="",cep_3=""),
    Row(cpf='11111122224', nome='Joao' ,ds_contato_1='31@timbrasil.com.br', ds_contato_2='32@timbrasil.com.br', ds_contato_3='33@timbrasil.com.br', ds_address_1="tim", ds_address_2="gmail",ds_address_3="",cep_1="",cep_2="",cep_3="")
])


df = df.withColumn('contato', array(col('ds_contato_1'), col('ds_contato_2'), col('ds_contato_3'))) \
        .withColumn('address', array(col('ds_address_1'),col('ds_address_2'),col('ds_address_3'))) \
        .withColumn('cep', array(col('cep_1'),col('cep_2'),col('cep_3')))
        
        
df = df.withColumn('contatos', arrays_zip(col('contato'),col('address'),col('cep')))

df.show(truncate=False)

df.printSchema()
