import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.ml.feature import Bucketizer

# Initialize Spark Session
spark = SparkSession.builder.appName("SingleDF_PSI").getOrCreate()

# Sample DataFrame creation
data = [
    (0.1, 'reference'), (0.2, 'reference'), (0.8, 'reference'), (0.4, 'reference'), (0.5, 'reference'),
    (0.6, 'reference'), (0.7, 'reference'), (0.8, 'reference'), (0.9, 'reference'), (0.9, 'reference'),
    (0.1, 'current'), (0.3, 'current'), (0.4, 'current'), (0.4, 'current'), (0.6, 'current'),
    (0.7, 'current'), (0.7, 'current'), (0.8, 'current'), (0.9, 'current'), (0.9, 'current'),
]
columns = ["score", "period_type"]
df = spark.createDataFrame(data, columns)

def calculate_psi_single_df(df, score_col, type_col, num_bins=10):
    """
    Calculates PSI using a single DataFrame with a type column.
    """
    num_bins=10
    score_col="score"
    type_col="period_type"
    
    quantiles = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    bucketizer = Bucketizer(
        splits=quantiles,
        inputCol="score",
        outputCol="bin_index"
    )

    df_binned = bucketizer.transform(df)
    #ref_df.select(F.histogram_numeric('score', F.lit(10))).show(truncate=False)

    # 3. Calculate Proportions
    # Use window functions to get total counts per period type
    window_spec = Window.partitionBy(type_col)
    df_proportions = df_binned.groupBy("bin_index", type_col).count() \
        .withColumn("total_count_type", F.sum("count").over(window_spec)) \
        .withColumn("percentage", F.col("count") / F.col("total_count_type"))

    # Pivot the data to get reference_pct and current_pct in the same row
    psi_df = df_proportions.groupBy("bin_index").pivot(type_col, ['reference', 'current']) \
        .agg(F.first("percentage")) \
        .fillna(0.0)
    
    # Rename pivoted columns
    psi_df = psi_df.withColumnRenamed("reference", "baseline_pct") \
                   .withColumnRenamed("current", "current_pct")

    # 4. Apply Formula
    # Filter out bins that have 0% in both to avoid log issues
    psi_df = psi_df.filter((F.col("baseline_pct") > 0) | (F.col("current_pct") > 0))

    psi_components = psi_df.withColumn(
        "psi_component",
        (F.col("current_pct") - F.col("baseline_pct")) * 
        F.log(F.col("current_pct") / F.col("baseline_pct"))
    ).fillna(0) # Handle cases where percentages are equal

    # 5. Sum PSI Components
    total_psi = psi_components.agg(F.sum("psi_component")).collect()[0][0]
    
    return total_psi

# Calculate PSI
total_psi_score = calculate_psi_single_df(df, "score", "period_type")
print(f"The total PSI score is: {total_psi_score}")
