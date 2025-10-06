# Databricks notebook source
df_hosa = spark.read.csv('/mnt/landing/claims/hospital1_claim_data.csv',header=True)
df_hosb = spark.read.csv('/mnt/landing/claims/hospital2_claim_data.csv',header=True)

# COMMAND ----------

from pyspark.sql import functions as F 
# Read only the existing CSV files
df = (
    spark.read
    .format("csv").option('header','True')
    .load('/mnt/landing/claims/*.csv')
)
df = df.withColumn('datasource',F.when(F.col('PatientID').contains('HOSP1'),'hosa').when(F.col('PatientID').contains('HOSP2'),'hosb'))

df.write.mode('overwrite').format('parquet').parquet('/mnt/bronze/claims/')