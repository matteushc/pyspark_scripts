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

# 1. Define the Metric Provider
class ColumnValuesToBeEqualCustom(ColumnMapMetricProvider):
    # Give your metric a name
    condition_metric_name = "column_values.equal_custom"
    condition_value_keys = ("param_value",)
    # Define the Spark logic using a partial function
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column: Column, **kwargs) -> Column:
        # The logic is a PySpark column expression that returns a boolean
        # This will be used to determine if a row is a "success"
        param_value = kwargs["param_value"]
        #query = F.when(column == value, F.lit(False)).otherwise(F.lit(True))
        return column > F.lit(param_value)

# 2. Define the Expectation
class ExpectColumnValuesToBeEqualCustom(ColumnMapExpectation):
    """Expect values in this column to be positive."""
    param_value: Optional[Comparable] = None
    condition_value_keys = ("param_value",)
    # Connect the Expectation to the Metric Provider
    map_metric = "column_values.equal_custom"
    success_keys = ("mostly","param_value",)
    # Define the default kwargs and success keys
    default_kwarg_values = {
        "mostly": 1.0,
        "param_value": 0
    }
    def validate_configuration(self, configuration: ExpectationConfiguration) -> None:
        super().validate_configuration(configuration)
        # Add your own custom validation logic here if needed
        if configuration.kwargs.get("param_value") is None:
            raise ValueError("Parameter 'param_value' cannot be None.")

# 3. (Optional) Run a diagnostic checklist if executed as a script
if __name__ == "__main__":
    ExpectColumnValuesToBeEqualCustom().print_diagnostic_checklist()
