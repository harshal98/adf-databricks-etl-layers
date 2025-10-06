# Databricks notebook source
df = spark.read.csv('/mnt/landing/cptcodes/cptcodes.csv',header=True)

# COMMAND ----------

colmapping = {}
for col in df.columns:
    colmapping[col]=col.replace(' ','_').lower()

df = df.withColumnsRenamed(colmapping)
display(df)

# COMMAND ----------



df.write.mode('overwrite').format('parquet').parquet('/mnt/bronze/cptcodes')