# Import required modules from GX library.
from pyspark.sql import SparkSession
import great_expectations as gx
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from great_expectations.expectations.expectation import ColumnAggregateExpectation
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.expectations.metrics import ColumnMapMetricProvider
from great_expectations.render.renderer.renderer import Renderer
from typing import Optional, Tuple

# Create Data Context.
context = gx.get_context()

data = [
    ("Alice", 30, "M"),
    ("Bob", 25, "M"),
    ("Cathy", 35, "F"),
    ("David", 5, "M"),  # Intentionally bad age for validation
    (None, 40, "F")     # Intentionally null name for validation
]
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True)
])

spark = SparkSession.builder.appName("GE_Spark_Validation").getOrCreate()
# Create the PySpark DataFrame
df = spark.createDataFrame(data, schema)
df.show()

data_source = context.data_sources.add_spark(name="Spark Data Source")
data_asset = data_source.add_dataframe_asset(name="Customer data")
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
#batch = batch_definition.get_batch()

# Create Expectation Suite containing two Expectations.
suite = context.suites.add_or_update(
    gx.core.expectation_suite.ExpectationSuite(name="expectations")
)

# 3. Register the custom expectation with Great Expectations
# This step is crucial for GE to recognize and use the class
# gx.expectations.registry.register_expectation(
#     ExpectColumnAggToBeBetweenCustom
# )

gx.expectations.registry.register_expectation(
    ExpectColumnValuesToBePositiveWithSpark
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="name", mostly=0.66, severity="warning"
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="age", min_value=0, severity="critical"
    )
)

#suite.add_expectation(ExpectColumnAggToBeBetweenCustom(column="age", min_value=1, max_value=40))

suite.add_expectation(ExpectColumnValuesToBePositiveWithSpark(column="age", param_value=25, mostly=1.0))

# Create Validation Definition.
validation_definition = context.validation_definitions.add_or_update(
    gx.core.validation_definition.ValidationDefinition(
        name="validation definition",
        data=batch_definition,
        suite=suite,
    )
)

# Create Checkpoint, run Checkpoint, and capture result.
checkpoint = context.checkpoints.add_or_update(
    gx.checkpoint.checkpoint.Checkpoint(
        name="checkpoint", validation_definitions=[validation_definition],
        result_format={
                "result_format": "COMPLETE"
            },
    )
)

checkpoint_result = checkpoint.run(batch_parameters={"dataframe": df})

print(checkpoint_result.describe())
