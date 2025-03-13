import argparse
import pyspark
from pyspark.sql.functions import date_format
from datetime import datetime
from decouple import config


BASE_PATH = "file:///opt/bitnami/app/data"


BD_SERVER_URL = config('BD_SERVER_URL')
BD_USER = config('BD_USER')
BD_PASSWORD = config('BD_PASSWORD')


QUERY_ALL_TABLES = """
	(
		select tablename 
		from pg_tables where schemaname = 'public'
	) t
"""

QUERY_ALL_DATES = """
(
	select distinct to_char(order_date, 'YYYYMM') order_date  
	from orders order by order_date
) t
"""

QUERY_MIN_MAX = """
	(
		select
			min({key}),
			max({key})
		from {table_jdbc}
	) p 
"""

QUERY_TABLE_ORDER = """
(
	select order_id,
		customer_id,
		employee_id,
		order_date,
		required_date,
		shipped_date,
		ship_via,
		freight,
		ship_name,
		ship_address,   
		ship_city,   
		ship_region,
		ship_postal_code,
		ship_country
	from {table_name}
	where {order_column} = '{date_format}'
) t
"""


def execute_query(query):
	return spark.read.format("jdbc") \
					.option("url", BD_SERVER_URL) \
					.option("dbtable", query) \
					.option("user", BD_USER) \
					.option("password", BD_PASSWORD) \
					.option("driver", "org.postgresql.Driver") \
					.load()


def load_table(query_table, partition_column=None):
	if partition_column:

		query_min_max = QUERY_MIN_MAX.format(
			key="order_id", table_jdbc='orders'
		)
		total_min_max_table = execute_query(query_min_max)

		minimum = int(total_min_max_table.first()[0])
		maximum = int(total_min_max_table.first()[1])

		return spark.read.format("jdbc") \
			.option("url", BD_SERVER_URL) \
			.option("lowerBound", minimum) \
			.option("upperBound", maximum) \
			.option("numPartitions", spark.sparkContext.defaultParallelism) \
			.option("partitionColumn", partition_column) \
			.option("dbtable", query_table) \
			.option("user", BD_USER) \
			.option("password", BD_PASSWORD) \
			.option("driver", "org.postgresql.Driver") \
			.load()
	else:
		return spark.read.format("jdbc") \
            .option("url", BD_SERVER_URL) \
            .option("numPartitions", spark.sparkContext.defaultParallelism) \
            .option("dbtable", query_table) \
            .option("user", BD_USER) \
            .option("password", BD_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()


def add_columns_dataframe(df):
	return df.withColumn(
				"year", date_format("order_date", "yyyy").cast("int")
			).withColumn(
				"month", date_format("order_date", "yyyyMM").cast("int")
			).withColumn(
				"day", date_format("order_date", "yyyyMMdd").cast("int")
			)


def load_csv_file(file_name):
	return spark.read.csv(
		file_name, 
		header=True,
		sep="," 
	)


def load_parquet_table(file_partition):
	return spark.read.option("basePath", BASE_PATH).parquet(*file_partition)


def save_parquet_files(df, table_name, partition=None):
	if partition:
		df.write.parquet(
			path=f"{output_tables}{table_name}",
			partitionBy=partition,
			mode='overwrite'
		)
	else:
		df.write.parquet(
			path=f"{output_tables}{table_name}",
			mode='overwrite'
		)
	


def save_results(df_final):

	properties = {
		"user": BD_USER,
		"password": BD_PASSWORD,
		"driver": "org.postgresql.Driver"
	}

	df_final.write.jdbc(
		BD_SERVER_URL, 
		"order_details_complete",
		properties=properties, mode="append"
	)

	df_final.coalesce(1).write.mode("overwrite").csv(
		f"{BASE_PATH}/result_data.csv",
		header=True
	)


def extract_all_tables(execution_day=None, list_dates=None):

	list_tables = execute_query(QUERY_ALL_TABLES).collect()

	for table in list_tables:
		if table.tablename == 'orders' and execution_day:

			bd_date_format = datetime.strptime(
				execution_day, "%Y%m%d"
			).strftime("%Y-%m-%d")

			query_table = QUERY_TABLE_ORDER.format(
				table_name=table.tablename,
				order_column="order_date",
				date_format=bd_date_format
			)

			df = load_table(query_table, "order_id")
			df = add_columns_dataframe(df)
			save_parquet_files(df, table.tablename, ['year', 'month', 'day'])

		elif table.tablename == 'orders':

			for date in list_dates:
				query_table = QUERY_TABLE_ORDER.format(
					table_name=table.tablename,
					order_column="to_char(order_date, 'YYYYMM')",
					date_format=date.order_date
				)
				df = load_table(query_table, "order_id")
				df = add_columns_dataframe(df)
				save_parquet_files(df, table.tablename, ['year', 'month', 'day'])
		
		else:
			df = load_table(table.tablename)
			save_parquet_files(df, table.tablename)


def get_partition_table():
	list_partitions = []
	if execution_day:
		data = datetime.strptime(execution_day,'%Y%m%d')
		year = data.strftime('%Y')
		year_month = data.strftime('%Y%m')
		partition = f"{BASE_PATH}/tables/orders/year={year}/month={year_month}/day={execution_day}"
		list_partitions.append(partition)
	else:
		for date in list_dates:
			partition = f"{BASE_PATH}/tables/orders/year={date.order_date[0:4]}/month={date.order_date}"
			list_partitions.append(partition)
	
	return list_partitions


if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="Pipeline BK")
	parser.add_argument('-ed','--executionDay', metavar='executionDay', type=str, help='')
	args = parser.parse_args()

	execution_day = args.executionDay

	output_tables = f"{BASE_PATH}/tables/"

	spark = pyspark.sql.session.SparkSession\
					.builder\
					.appName("Pipeline extract database")\
					.enableHiveSupport()\
					.getOrCreate()

	spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")					

	list_dates = None
	if not execution_day:
		list_dates = execute_query(QUERY_ALL_DATES).collect()
	
	extract_all_tables(execution_day, list_dates)

	df_order_details = load_csv_file(f"{BASE_PATH}/order_details.csv")
	df_order_details.createOrReplaceTempView("order_detail")

	list_partitions = get_partition_table()

	df_order = load_parquet_table(list_partitions)

	df_order.createOrReplaceTempView("order")

	df_final = spark.sql("""
		select 	o.order_id,
				o.customer_id,
				o.employee_id,
				o.order_date,
				o.required_date,
				o.shipped_date,
				o.ship_via,
				o.freight,
				o.ship_name,
				o.ship_address,   
				o.ship_city,   
				o.ship_region,
				o.ship_postal_code,
				o.ship_country,
				od.unit_price,
				od.quantity,
				od.discount
		from order o
		join order_detail od
		on o.order_id = od.order_id
	""")

	save_results(df_final)