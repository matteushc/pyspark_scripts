from typing import Dict, Optional

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import MetricPartialFunctionTypes
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    ExpectationValidationResult,
    render_suite_parameter_string,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.core.types import Comparable
from great_expectations.expectations.metrics.metric_provider import metric_partial
from great_expectations.render import (
    CollapseContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import num_to_str
from great_expectations.validator.metric_configuration import MetricConfiguration

result_dict = {}

class ColumnValuesEqualValue(ColumnMapMetricProvider):
    metric_name = "column_values.custom_values_equal_value"
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
        column_name = accessor_domain_kwargs["column"]
        #value = metric_value_kwargs["value"]
        column = F.col(column_name)
        query = F.when(column == 3, F.lit(False)).otherwise(F.lit(True))
        return (query, compute_domain_kwargs, accessor_domain_kwargs)
    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[Dict] = None,
    ):
        """Returns a dictionary of given metric names and their corresponding configuration, specifying the metric
        types and their respective domains"""
        dependencies: Dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        table_domain_kwargs: Dict = {
            k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
        }
        dependencies["table.column_types"] = MetricConfiguration(
            metric_name="table.column_types",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs={
                "include_nested": True,
            },
        )
        return dependencies


# This class defines the Expectation itself
# <snippet name="docs/docusaurus/docs/snippets/expect_column_values_to_equal_three.py ExpectColumnValuesToEqualThree class_def">
class ExpectColumnValuesToEqualThree(ColumnMapExpectation):
    #value: Optional[Comparable] = None
    map_metric = "column_values.custom_values_equal_value"
    success_keys = ("mostly",)
    
    # def validate_configuration(self, configuration: ExpectationConfiguration) -> None:
    #     super().validate_configuration(configuration)
    #     # Add any custom configuration validation here
    #     if "value" not in configuration.kwargs:
    #         raise ValueError("value is a required argument.")