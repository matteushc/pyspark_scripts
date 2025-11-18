from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility import pyspark
from pyspark.sql import Column
from great_expectations.core.types import Comparable
from typing import Dict, Optional
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.core.metric_function_types import MetricPartialFunctionTypes
from great_expectations.expectations.metrics.metric_provider import metric_partial
from great_expectations.core.metric_domain_types import MetricDomainTypes
from pyspark.sql.window import Window
from pyspark.ml.feature import Bucketizer
import pyspark.sql.functions as F

# 1. Define the Metric Provider
class ColumnPsiMetric(ColumnMapMetricProvider):
    # Give your metric a name
    condition_metric_name = "column_values.psi_metric"
    condition_value_keys = ("param_value",)

    @metric_partial(
        engine=SparkDFExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.MAP_CONDITION_FN,
        domain_type=MetricDomainTypes.COLUMN,
    )
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):

        (
            _,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )

        spark_df = execution_engine.dataframe
        #print(spark_df.dataframe.show())
        
        param_value = metric_value_kwargs["param_value"]
        
        column_name = accessor_domain_kwargs["column"]
        column = F.col(column_name)

        type_col="period_type"
        
        bucketizer = Bucketizer(
            splits=param_value,
            inputCol="score",
            outputCol="bin_index"
        )

        df_binned = bucketizer.transform(spark_df)

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
        
        #query = F.when(column == param_value, F.lit(False)).otherwise(F.lit(True))
        query = None

        return (query, compute_domain_kwargs, accessor_domain_kwargs)


class ExpectColumnPsiMetric(ColumnMapExpectation):
    """Expect values in this column to be positive."""
    param_value: Optional[Comparable] = None
    condition_value_keys = ("param_value",)

    map_metric = "column_values.psi_metric"
    success_keys = ("mostly","param_value",)

    default_kwarg_values = {
        "mostly": 1.0,
        "param_value": 0
    }
    def validate_configuration(self, configuration: ExpectationConfiguration) -> None:
        super().validate_configuration(configuration)
        # Add your own custom validation logic here if needed
        if configuration.kwargs.get("param_value") is None:
            raise ValueError("Parameter 'param_value' cannot be None.")
