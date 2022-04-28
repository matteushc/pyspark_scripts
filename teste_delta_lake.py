from delta.tables import *
from pyspark.sql.functions import col
from datetime import datetime


def create_delta_table_s3():

    data = spark.createDataFrame(
        [
            (1, "teste_1"),
            (2, "teste_2"),
            (3, "teste_3"),
            (4, "teste_4"),
            (5, "teste_5")
        ],
        ["id", "label"]  
    )

    newData = spark.createDataFrame(
        [
            (1, "old_teste_1"),
            (3, "old_teste_3"),
            (6, "teste_6")
        ],
        ["id", "label"]
    )

    # Save s3
    data.write.format("delta").save("s3://delta-lake/delta-table/")
    # Load s3
    deltaTable = DeltaTable.forPath(spark, "s3://delta-lake/delta-table/")


    # Conditional update without overwrite - upsert
    deltaTable.alias("oldData") \
    .merge(
        newData.alias("newData"),
        "oldData.id = newData.id") \
    .whenMatchedUpdate(set = { "label": col("newData.label") }) \
    .whenNotMatchedInsert(values = 
            {
            "id": "newData.id",
            "label": "newData.label"
            }
        ) \
    .execute()


    df = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("s3://delta-lake/delta-table")


def create_delta_table_locally():

    df = spark.createDataFrame(
        lista_dados,
        ["id", "label", "description", "date"]
    )

    lista_dados = []
    for x in range(1000000):
        data = datetime.strptime("2022-04-26", "%Y-%m-%d")
        data = (x, f"foo_{x}", f"Data extracted from API with label number: {x}", data)
        lista_dados.append(data)

    # Save locally
    df.write.format("delta").save("file:///path/teste_delta_lake/delta-table")

    deltaTable = DeltaTable.forPath(spark, "file:///path/teste_delta_lake/delta-table")

    novaData = datetime.strptime("2022-04-24", "%Y-%m-%d")
    newData = spark.createDataFrame(
        [
            (1, "foo_foo1", "Data loaded from server number: 1", novaData),
            (3, "bar_3", "Save", novaData),
            (6, "teste_6", "Save", novaData)
        ],
        ["id", "label", "description", "date"]
    )


    deltaTable.alias("oldData") \
    .merge(
        newData.alias("newData"),
        "oldData.id = newData.id") \
    .whenMatchedUpdate(set = { 
            "label": col("newData.label"),
            "description": col("newData.description"),
            "date": col("newData.date")
        }) \
    .whenNotMatchedInsert(values = 
            {
            "id": "newData.id",
            "label": "newData.label"
            }
        ) \
    .execute()


    df = spark.read.format("delta").load("file:///path/teste_delta_lake/delta-table")
    df.show()

    df = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("file:///path/teste_delta_lake/delta-table")

    df.show()


def create_delta_table_locally_partition():

    from datetime import datetime

    df = spark.createDataFrame(
        lista_dados,
        ["id", "label", "description", "date"]
    )

    lista_dados = []
    for x in range(100000):
        if x % 2 == 0:
            data_str = f"2022-04-10"
        else:
            data_str = f"2022-04-12"
        data = (x, f"foo_{x}", f"Data extracted from API with label number: {x}", data_str)
        lista_dados.append(data)

    df.write.format("delta").partitionBy("date").save("file:///path/teste_delta_lake/delta-partition")

    newData = spark.createDataFrame(
        [
            (1, "foo_foo1", "load", "2022-04-05"),
            (100001, "foo_100001", "load", "2022-04-05")
        ],
        ["id", "label", "description", "date"]
    )

    deltaTable = DeltaTable.forPath(spark, "file:///path/teste_delta_lake/delta-partition")


    deltaTable.alias("oldData") \
    .merge(
        newData.alias("newData"),
        "oldData.id = newData.id") \
    .whenMatchedUpdate(set = { 
            "label": col("newData.label"),
            "description": col("newData.description"),
            "date": col("newData.date")
        }) \
    .whenNotMatchedInsert(values = 
            {
                "id": "newData.id",
                "label": "newData.label",
                "description": "newData.description",
                "date": "newData.date"
            }
        ) \
    .execute()

    df = spark.read.format("delta").load("file:///path/teste_delta_lake/delta-partition")
    df.show()
