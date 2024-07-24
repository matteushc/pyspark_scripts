import pyspark.sql.functions as F
from pyspark.sql.functions import col, explode, max, count, when
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


data = [
    Row(uuid='xp9', app=65, first_app=2809, app_id=2809, total=10),
    Row(uuid='xp9', app=65, first_app=None, app_id=2767, total=10),
    Row(uuid='XT8', app=48, first_app=2809 ,app_id=2767, total=6),
    Row(uuid='XT8', app=48, first_app=None ,app_id=2809, total=4),
    Row(uuid='wr56', app=48, first_app=None ,app_id=2767, total=20),
    Row(uuid='wr56', app=48, first_app=None ,app_id=2809, total=20)
]

df = spark.createDataFrame(data)

df1 = df.groupby("uuid", "app", "first_app", "app_id") \
    .sum("total").withColumnRenamed("sum(total)", "total")

df2 = df1.groupby("uuid", "app", "first_app")\
         .agg(
             F.map_from_entries(
                 F.collect_list(F.struct("app_id","total"))
                ).alias("app"))


spec  = Window.partitionBy("uuid", "app")
spec_4  = Window.partitionBy("uuid", "app")
spec_2  = Window.partitionBy("uuid", "app").orderBy("value")
spec_3  = Window.partitionBy("uuid", "app", "value")

df_explode = df2.select(
    col("uuid"), col("app"), col("first_app"), explode("app"))

df_explode.createOrReplaceTempView("df_explode")
    
df_rows = df_explode.withColumn("max_value", max("value").over(spec)) \
    .withColumn("first_app", max("first_app").over(spec_4)) \
    .withColumn("number_line", row_number().over(spec_2)) \
    .withColumn("transactions_equals_count", count(when(col("value") == col("max_value"), True)).over(spec_3))

df_t =  spark.sql("""
    select  uuid,
            app,
            key,
            value,
            max(value) OVER(PARTITION BY uuid,
                        app) max_value,
            max(first_app) OVER(PARTITION BY uuid, app) first_app,
            row_number() OVER(PARTITION BY uuid,
                        app order by value) row_number
    from df_explode
""")
df_t.createOrReplaceTempView("t1")

df_t2 = spark.sql("""
    select  uuid,
            app,
            key,
            value,
            max_value,
            first_app,
            row_number,
            count(case when value = max_value then True end) 
                OVER(PARTITION BY uuid, app, value) transactions_equals_count
    from t1
""")


df_result_1 = df_rows.where("transactions_equals_count > 1 and first_app is not null") \
        .select("uuid", "app", "first_app").distinct()


df_result_2 = df_rows.where("transactions_equals_count > 1 and first_app is null and number_line = 1") \
        .select("uuid", "app", col("key").alias("first_app")).distinct()

df_result_3 = df_rows.where("transactions_equals_count = 1 and max_value = value") \
    .select("uuid", "app", col("key").alias("first_app")).distinct()

df_result_4 = df_rows.where("transactions_equals_count > 2 and max_value = value") \
    .select("uuid", "app", col("key").alias("first_app")).distinct()

df_result = df_rows.where("max_value = value")
