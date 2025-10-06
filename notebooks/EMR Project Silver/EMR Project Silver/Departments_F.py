# Databricks notebook source
df_hosa = spark.read.parquet('/mnt/bronze/hosa/departments')
df_hosb = spark.read.parquet('/mnt/bronze/hosb/departments')

df = df_hosa.union(df_hosb)

cols = {}
for col in df.columns:
    cols[col] = col.lower().replace(' ','')

df_renamedcols = df.withColumnsRenamed(cols)

# COMMAND ----------

from pyspark.sql import functions as F
df2 = df_renamedcols.withColumn('src_deptid',F.concat(F.col("deptid"),F.lit('-'),F.col("datasource")))

#df2 = df2.drop('deptid')

d2 = df2.withColumn('is_quarentiend',F.when(F.col("src_deptid").isNull() | F.col("name").isNull(),True).otherwise(False))

d2.createOrReplaceTempView("departments")

spark.sql('select * from departments').display()


# COMMAND ----------

 #Define schema
 from pyspark.sql.types import StructType, StructField, StringType, BooleanType
schema = StructType([
    StructField("Dept_Id", StringType(), True),
    StructField("SRC_Dept_Id", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("datasource", StringType(), True),
    StructField("is_quarantined", BooleanType(), True)
])

# Create empty DataFrame
empty_df = spark.createDataFrame([], schema)


# Save as Delta table only if it does not exist
empty_df.write.format("delta") \
    .mode("ignore") \
    .saveAsTable("silver.departments")

spark.sql('Truncate table silver.departments')

# COMMAND ----------

# MAGIC %sql
# MAGIC  insert into silver.departments
# MAGIC  SELECT 
# MAGIC   deptid as Dept_Id,
# MAGIC  src_deptid as SRC_Dept_Id,
# MAGIC  name as Name,
# MAGIC  datasource as Datasource,
# MAGIC is_quarentiend as is_quarantined
# MAGIC  FROM departments
# MAGIC