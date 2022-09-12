## SOURCE
## https://dwgeek.com/spark-sql-recursive-dataframe-pyspark-and-scala.html/

from pyspark.sql.functions import *

# Employee DF
schema = 'EMPLOYEE_NUMBER int, MANAGER_EMPLOYEE_NUMBER int'
employees = spark.createDataFrame(
[[801,None], 
[1016,801], 
[1003,801], 
[1019,801], 
[1010,1003], 
[1005,1003], 
[1004,1005],
[1001,1003], 
[1012,1004], 
[1002,1004], 
[1015,1004], 
[1008,1019], 
[1006,1019], 
[1014,1019], 
[1011,1019]], schema=schema)

schema_cargo = 'level_0 int, cargo string'
cargos = spark.createDataFrame(
[[801, 'diretor'], 
[1016, 'emp'], 
[1003, 'gerente'], 
[1019, 'gerente'], 
[1010, 'emp'], 
[1005, 'supervisor'], 
[1004, 'supervisor'],
[1001, 'emp'], 
[1012, 'emp'], 
[1002, 'emp'], 
[1015, 'emp'], 
[1008, 'emp'], 
[1006, 'emp'], 
[1014, 'emp'], 
[1011, 'emp']], schema=schema_cargo)

# initial DataFrame
empDF = employees \
  .withColumnRenamed('EMPLOYEE_NUMBER', 'level_0') \
  .withColumnRenamed('MANAGER_EMPLOYEE_NUMBER', 'level_1')
empDF = empDF.withColumn('supervisor', lit(None)).withColumn('gerente', lit(None)).withColumn('diretor', lit(None))

i = 1

# Loop Through if you dont know recusrsive depth
while True:
  old_level =  'level_{}'.format(i-1)
  this_level = 'level_{}'.format(i)
  next_level = 'level_{}'.format(i+1)
  emp_level = employees \
    .withColumnRenamed('EMPLOYEE_NUMBER', this_level) \
    .withColumnRenamed('MANAGER_EMPLOYEE_NUMBER', next_level)
  cargos = cargos.withColumnRenamed(old_level, this_level)
  empDF = empDF.join(emp_level, on=this_level, how='left').join(cargos.alias(this_level), on=this_level, how='left')
  empDF = empDF.withColumn('supervisor', when(col(this_level + ".cargo") == "supervisor", concat_ws(';', col("supervisor"), col(this_level))).otherwise(col("supervisor")))
  empDF = empDF.withColumn('gerente', when(col(this_level + ".cargo") == "gerente", concat_ws(';', col("gerente"), col(this_level))).otherwise(col("gerente")))
  empDF = empDF.withColumn('diretor', when(col(this_level + ".cargo") == "diretor", concat_ws(';', col("diretor"), col(this_level))).otherwise( col("diretor")))
  # Check if DF is empty. Break loop if empty, Otherwise continue with next level
  if empDF.where(col(next_level).isNotNull()).count() == 0:
    break
  else:
    i += 1

# Sort columns and show
empDF.sort('level_0').select('level_0').show()

empDF.select(["level_0", "supervisor", "gerente", "diretor"]) \
.groupBy("level_0").agg(
    concat_ws(';', collect_set(col("supervisor"))), 
    concat_ws(';', collect_set(col("gerente"))), 
    concat_ws(';', collect_set(col("diretor")))).show()
