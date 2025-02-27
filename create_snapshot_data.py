import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('test').getOrCreate()

historic_foto_df = spark.read.format("org.apache.spark.sql.csv")
                  .load("/path/data.csv")

base_table = spark.read.format("org.apache.spark.sql.csv")
                  .load("/path/data.csv")

key = ['id']
full_historic = historic_foto_df.join(base_table, key, "fullouter").select(key + [
    coalesce(base_table[c], historic_foto_df[c]).alias(c) for c in historic_foto_df.columns if c not in key
])

full_historic.write.option("header",True) \
 .csv("/tmp/spark_output/data")
