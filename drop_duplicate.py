from datetime import datetime
from pyspark.sql.functions import col, max as _max
from pyspark.sql.window import Window

df = spark.createDataFrame(
    [
        ('5549999641561', datetime.strptime("2023-12-18 12:22:13", "%Y-%m-%d %H:%M:%S"), "-14.90", "20241012_RAW_DATA.txt", datetime.strptime("2024-10-13 03:14:50", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2024-10-13 11:07:57", "%Y-%m-%d %H:%M:%S"), "2024-10-12"),
        ('5549999641561', datetime.strptime("2024-05-13 12:39:08", "%Y-%m-%d %H:%M:%S"), "-12.90", "20241012_RAW_DATA.txt", datetime.strptime("2024-10-13 03:14:50", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2024-10-13 11:07:57", "%Y-%m-%d %H:%M:%S"), "2024-10-12")
        
    ],
    ["cd_id", "ts_last_transaction_date", "vl_price", "nm_arquivo", "dt_arquivots", "ts_currentdate", "dat_ref"]
)

## Drop duplicate one way
partition_columns =  ["cd_id", "nm_arquivo", "dt_arquivots", "ts_currentdate", "dat_ref"]
df_result = df.orderBy(col("ts_last_transaction_date").desc())\
   .dropDuplicates(partition_columns)
   
df_result.show()

## Drop duplicate another way
df_result = df.withColumn('max', _max('ts_last_transaction_date').over(Window.partitionBy(partition_columns)))\
        .where(col('ts_last_transaction_date') == col('max')) \
        .drop(col("max"))
        
