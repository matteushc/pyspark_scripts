from pyspark.sql.functions import from_json

directory = "/teste.json"
df = spark.read.format("json").option("multiline", "true").option("inferSchema","true").load(directory)

df_json = df.select("json")

json_schema = sqlContext.read.json(df_json.rdd.map(lambda r: r.json)).schema

df_2 = df.withColumn("json", from_json(col('data').cast("string"), json_schema))
