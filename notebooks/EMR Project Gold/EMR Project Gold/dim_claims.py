# Databricks notebook source
# MAGIC %sql
# MAGIC select * 
# MAGIC from silver.claims

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_claims (
# MAGIC     ServiceDate DATE,
# MAGIC     ClaimDate DATE,
# MAGIC     PayorID String,
# MAGIC     ClaimAmount DECIMAL(12,2),
# MAGIC     PaidAmount DECIMAL(12,2),
# MAGIC     ClaimStatus VARCHAR(50),
# MAGIC     PayorType VARCHAR(50),
# MAGIC     Deductible DECIMAL(12,2),
# MAGIC     Coinsurance DECIMAL(12,2),
# MAGIC     Copay DECIMAL(12,2),
# MAGIC     SRC_ModifiedDate Date)

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.dim_claims

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into gold.dim_claims
# MAGIC Select ServiceDate,
# MAGIC ClaimDate,
# MAGIC PayorID,
# MAGIC ClaimAmount,
# MAGIC PaidAmount,
# MAGIC ClaimStatus,
# MAGIC PayorType,
# MAGIC Deductible,
# MAGIC Coinsurance,
# MAGIC Copay,
# MAGIC SRC_ModifiedDate
# MAGIC from silver.claims