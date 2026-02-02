from pyspark.sql.functions import col, lit, array, struct, when

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.getOrCreate()

# ---------------------------
# 1) Exemplo de Spark DataFrame
# ---------------------------
data = [
    ("median", "low",  "age", 0.10, 0.08, 0.02),
    ("median", "mid",  "age", 0.50, 0.55, -0.05),
    ("median", "high", "age", 0.40, 0.37, 0.03),

    # first_quartil -> só mapeia result→first_quartil
    ("first_quartil",          "low",  "age", 0.02, 0.01, 0.01),
    ("first_quartil",          "mid",  "age", 0.03, 0.02, 0.01),

    ("count", "N/A",  "clicks", 1200.0, 1000.0, 200.0),
    ("count", "N/A",  "views",  5000.0, 5200.0,-200.0),

    # outro statistic qualquer -> cai em 'except'
    ("stability",    "all",  "feature", 0.75, 0.73, 0.02),
]

columns = ["statistic", "range", "column_name", "value", "previous_value", "variation"]
spark_df = spark.createDataFrame(data, schema=columns)


df_piv = spark_df.withColumn("tab", 
                    array([struct( lit(x).alias("y"), col(x).alias("z") ) 
                    for x in ["value", "previous_value", "variation"] ]) ) \
                    .selectExpr("*", "inline(tab)").drop("tab")

df_select = df_piv.withColumn("y", 
            when( (col("statistic") == lit("median")) & ( col("y") == lit("value") ) , lit("current") ) \
            .when( (col("statistic") == lit("median")) & (col("y") == lit("previous_value") ), lit("last")) \
            .when( (col("statistic") == lit("median")) & (col("y") == lit("difference") ) , lit("variation")) \
            .when( (col("statistic") == lit("first_quartil")) & (col("y") == lit("value") ) , lit("first_quartil") )\
            .when( (col("statistic") == lit("count")) & (col("y") == lit("value") ), lit("current_count")) \
            .when( (col("statistic") == lit("count")) & (col("y") == lit("previous_value") ), lit("last_count")) \
            .otherwise(lit("except") ) )

df_value = df_select.select("range", "statistic", "column_name", "y", "z")
final = df_value.groupby("range", "column_name").pivot("y").agg(F.first("z"))
