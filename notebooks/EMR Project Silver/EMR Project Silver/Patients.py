# Databricks notebook source
df_hosa = spark.read.format('parquet').load('/mnt/bronze/hosa/patients')
df_hosb = spark.read.format('parquet').load('/mnt/bronze/hosb/patients')

# COMMAND ----------

df_hosb = df_hosb.withColumnsRenamed({
     "ID": "PatientID",
    "F_Name": "FirstName",
    "L_Name": "LastName",
    "M_Name": "MiddleName",
    "Updated_Date": "ModifiedDate",
})

# COMMAND ----------

df_merged = df_hosa.union(df_hosb)

display(df_merged)

# COMMAND ----------

from pyspark.sql import functions as F

df_cdm = df_merged.withColumnRenamed('PatientID','SRC_PatientID') \
        .withColumn('Patient_Key',
        F.concat(F.col('SRC_PatientID'), F.lit('-'), F.col('datasource')))\
        .drop('PatientID')

df_cdm = df_cdm.withColumn(
    'is_quarantined',
    F.when(
        F.col('SRC_PatientID').isNull() |
        F.col('dob').isNull() |
        F.col('FirstName').isNull() |
        (F.lower(F.col('FirstName')) == 'null'),
        True
    ).otherwise(False)
)

# COMMAND ----------

display(df_cdm)
df_cdm.createOrReplaceTempView('patients_cdm')

# COMMAND ----------

# MAGIC  %sql
# MAGIC  CREATE TABLE IF NOT EXISTS silver.patients (
# MAGIC      Patient_Key STRING,
# MAGIC      SRC_PatientID STRING,
# MAGIC      FirstName STRING,
# MAGIC      LastName STRING,
# MAGIC      MiddleName STRING,
# MAGIC      SSN STRING,
# MAGIC      PhoneNumber STRING,
# MAGIC      Gender STRING,
# MAGIC      DOB DATE,
# MAGIC      Address STRING,
# MAGIC      SRC_ModifiedDate TIMESTAMP,
# MAGIC      datasource STRING,
# MAGIC      is_quarantined BOOLEAN,
# MAGIC      inserted_date TIMESTAMP,
# MAGIC      modified_date TIMESTAMP,
# MAGIC      is_current BOOLEAN
# MAGIC  )
# MAGIC
# MAGIC   USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into silver.patients as target
# MAGIC using patients_cdm as source
# MAGIC ON target.Patient_Key = source.Patient_Key
# MAGIC AND target.is_current = true 
# MAGIC WHEN MATCHED
# MAGIC AND (
# MAGIC      target.SRC_PatientID <> source.SRC_PatientID OR
# MAGIC      target.FirstName <> source.FirstName OR
# MAGIC      target.LastName <> source.LastName OR
# MAGIC      target.MiddleName <> source.MiddleName OR
# MAGIC      target.SSN <> source.SSN OR
# MAGIC      target.PhoneNumber <> source.PhoneNumber OR
# MAGIC      target.Gender <> source.Gender OR
# MAGIC      target.DOB <> source.DOB OR
# MAGIC      target.Address <> source.Address OR
# MAGIC      target.SRC_ModifiedDate <> source.ModifiedDate OR
# MAGIC      target.datasource <> source.datasource OR
# MAGIC      target.is_quarantined <> source.is_quarantined
# MAGIC )
# MAGIC Then Update SET
# MAGIC     target.is_current = false,
# MAGIC     target.modified_date = current_timestamp()
# MAGIC
# MAGIC WHEN Not MATCHED 
# MAGIC THEN INSERT (
# MAGIC      Patient_Key,
# MAGIC      SRC_PatientID,
# MAGIC      FirstName,
# MAGIC      LastName,
# MAGIC      MiddleName,
# MAGIC      SSN,
# MAGIC      PhoneNumber,
# MAGIC      Gender,
# MAGIC      DOB,
# MAGIC      Address,
# MAGIC      SRC_ModifiedDate,
# MAGIC      datasource,
# MAGIC      is_quarantined,
# MAGIC      inserted_date,
# MAGIC      modified_date,
# MAGIC      is_current
# MAGIC  )
# MAGIC  VALUES (
# MAGIC      source.Patient_Key,
# MAGIC      source.SRC_PatientID,
# MAGIC      source.FirstName,
# MAGIC      source.LastName,
# MAGIC      source.MiddleName,
# MAGIC      source.SSN,
# MAGIC      source.PhoneNumber,
# MAGIC      source.Gender,
# MAGIC      source.DOB,
# MAGIC      source.Address,
# MAGIC      source.ModifiedDate,
# MAGIC      source.datasource,
# MAGIC      source.is_quarantined,
# MAGIC      current_timestamp(), -- Set inserted_date to current timestamp
# MAGIC      current_timestamp(), -- Set modified_date to current timestamp
# MAGIC      true -- Mark as current
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into silver.patients as target
# MAGIC using patients_cmd as source
# MAGIC ON target.Patient_Key = source.Patient_Key
# MAGIC AND target.is_current = true 
# MAGIC When not matched
# MAGIC THEN INSERT(
# MAGIC      Patient_Key,
# MAGIC      SRC_PatientID,
# MAGIC      FirstName,
# MAGIC      LastName,
# MAGIC      MiddleName,
# MAGIC      SSN,
# MAGIC      PhoneNumber,
# MAGIC      Gender,
# MAGIC      DOB,
# MAGIC      Address,
# MAGIC      SRC_ModifiedDate,
# MAGIC      datasource,
# MAGIC      is_quarantined,
# MAGIC      inserted_date,
# MAGIC      modified_date,
# MAGIC      is_current
# MAGIC  )
# MAGIC  VALUES (
# MAGIC      source.Patient_Key,
# MAGIC      source.SRC_PatientID,
# MAGIC      source.FirstName,
# MAGIC      source.LastName,
# MAGIC      source.MiddleName,
# MAGIC      source.SSN,
# MAGIC      source.PhoneNumber,
# MAGIC      source.Gender,
# MAGIC      source.DOB,
# MAGIC      source.Address,
# MAGIC      source.ModifiedDate,
# MAGIC      source.datasource,
# MAGIC      source.is_quarantined,
# MAGIC      current_timestamp(), -- Set inserted_date to current timestamp
# MAGIC      current_timestamp(), -- Set modified_date to current timestamp
# MAGIC      true -- Mark as current
# MAGIC  );