Auto
from __future__ import annotations

from typing import Dict, Optional
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.expectation import BatchExpectation
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

from great_expectations.core.metric_function_types import MetricPartialFunctionTypes
from great_expectations.expectations.metrics.metric_provider import metric_partial
from great_expectations.core.metric_domain_types import MetricDomainTypes


from pyspark.sql.window import Window
from pyspark.ml.feature import Bucketizer
import pyspark.sql.functions as F



class BatchColumnsUnique(TableMetricProvider):

    metric_name = "table.columns.unique"
    
    value_keys = ("column", "bins", "column_type", "baseline_pct", "current_pct",)

   
    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        
        df, compute_domain_kwargs, accessor_domain_kwargs = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.TABLE
        )

        bins = metric_value_kwargs["bins"]
        column_type = metric_value_kwargs["column_type"]
        baseline_pct = metric_value_kwargs["baseline_pct"]
        current_pct = metric_value_kwargs["current_pct"]
        column_name = metric_value_kwargs["column"]
        print(metric_value_kwargs)

        #bins = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        #column_type="period_type"
        #column_name="score"

        bucketizer = Bucketizer(
            splits=bins,
            inputCol=column_name,
            outputCol="bin_index"
        )

        df_binned = bucketizer.transform(df)

        # 3. Calculate Proportions
        # Use window functions to get total counts per period type
        window_spec = Window.partitionBy(column_type)
        df_proportions = df_binned.groupBy("bin_index", column_type).count() \
            .withColumn("total_count_type", F.sum("count").over(window_spec)) \
            .withColumn("percentage", F.col("count") / F.col("total_count_type"))

        # Pivot the data to get reference_pct and current_pct in the same row
        psi_df = df_proportions.groupBy("bin_index").pivot(column_type, ['reference', 'current']) \
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
        ).fillna(0)
        
        return psi_components

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        
        return {
            "table.columns": MetricConfiguration(
                "table.columns", metric.metric_domain_kwargs
            ),
        }

class ExpectBatchColumnsToBeUnique(BatchExpectation):
    
    column: str = None
    bins: list = None
    column_type: str = None
    baseline_pct: str = None
    current_pct: str = None

    #condition_value_keys = ("column",)
    value_keys = ("column", "bins", "column_type", "baseline_pct", "current_pct",)
    metric_dependencies = ("table.columns.unique", "table.columns")

    #success_keys = ("strict", "column", "bins", "column_type", "baseline_pct", "current_pct",)
    #success_keys = ("strict", "column1", "bins",)
    success_keys = ("strict", "column", "bins", "column_type", "baseline_pct", "current_pct",)

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: dict | None = None,
        execution_engine: ExecutionEngine | None = None,
    ):
        psi_components = metrics.get("table.columns.unique")
        #spark_df = execution_engine.dataframe
        dict_list = [row.asDict() for row in psi_components.collect()]
        
        return {
            "success": True,
            "result": {"observed_value": dict_list}
        }

    library_metadata = {
        "tags": ["uniqueness"],
        "contributors": ["@joegargery"],
    }
