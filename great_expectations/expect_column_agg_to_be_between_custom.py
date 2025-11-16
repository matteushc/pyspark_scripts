from great_expectations.expectations.metrics import ColumnMapMetricProvider, ColumnAggregateMetricProvider, column_aggregate_value
from great_expectations.expectations.expectation import ColumnAggregateExpectation, ColumnAggregateExpectation
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.core.types import Comparable
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
import pyspark.sql.functions as F
from great_expectations.render.renderer.renderer import renderer
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_value,
    column_aggregate_partial
)
from great_expectations.expectations.expectation import (
    ColumnAggregateExpectation,
    ExpectationValidationResult,
    render_suite_parameter_string,
)
from great_expectations.render import RenderedStringTemplateContent
from typing import Dict, Optional

class ColumnCustomAgg(ColumnAggregateMetricProvider):
    metric_name = "column.custom_max"
    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        #return column.agg(F.mean(column.cast("float"))).collect()[0][0]
        return F.mean(column.cast("float"))


class ExpectColumnAggToBeBetweenCustom(ColumnAggregateExpectation):
    examples = []
    min_value: Optional[Comparable] = None
    max_value: Optional[Comparable] = None
    strict_min: bool = False
    strict_max: bool = False
    metric_dependencies = ("column.custom_max",)
    success_keys = ("min_value", "strict_min", "max_value", "strict_max")
    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration
        configuration = configuration or self.configuration
        min_value = configuration.kwargs["min_value"]
        max_value = configuration.kwargs["max_value"]
        strict_min = configuration.kwargs["strict_min"]
        strict_max = configuration.kwargs["strict_max"]
        try:
            assert min_value is not None or max_value is not None, (
                "min_value and max_value cannot both be none"
            )
            assert min_value is None or isinstance(min_value, (float, int)), (
                "Provided min threshold must be a number"
            )
            assert max_value is None or isinstance(max_value, (float, int)), (
                "Provided max threshold must be a number"
            )
            if min_value and max_value:
                assert min_value <= max_value, (
                    "Provided min threshold must be less than or equal to max threshold"
                )
            assert strict_min is None or isinstance(strict_min, bool), (
                "strict_min must be a boolean value"
            )
            assert strict_max is None or isinstance(strict_max, bool), (
                "strict_max must be a boolean value"
            )
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))    
    def _validate(self,metrics: Dict, runtime_configuration: Optional[dict] = None, execution_engine: ExecutionEngine = None):
        column_max = metrics["column.custom_max"]
        # Obtaining components needed for validation
        success_kwargs = self._get_success_kwargs()
        min_value = success_kwargs.get("min_value")
        max_value = success_kwargs.get("max_value")
        strict_min = success_kwargs.get("strict_min")
        strict_max = success_kwargs.get("strict_max")
        # Checking if mean lies between thresholds
        if min_value is not None:
            if strict_min:
                above_min = column_max > min_value
            else:
                above_min = column_max >= min_value
        else:
            above_min = True
        if max_value is not None:
            if strict_max:
                below_max = column_max < max_value
            else:
                below_max = column_max <= max_value
        else:
            below_max = True
        success = above_min and below_max
        return {"success": success, "result": {"observed_value": column_max}}
    library_metadata = {
        "tags": ["flexible max comparisons"],
        "contributors": ["@joegargery"],
    }