# Databricks notebook source
# Drop the stale silver schema entirely from the metastore
spark.sql("DROP SCHEMA IF EXISTS silver CASCADE")

# Also clean up any leftover Delta folders from previous attempts
dbutils.fs.rm("abfss://bronzelayer@j2dstorage101.dfs.core.windows.net/silver/", True)

print("Cleanup done")

# COMMAND ----------

# Databricks notebook source

# Define base path once — easy to change later
SILVER_BASE = "abfss://bronzelayer@j2dstorage101.dfs.core.windows.net/silver"

# COMMAND ----------

# COMMAND ----------
# Create schema pointing to bronzelayer (NOT silverlayer)
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS silver
LOCATION '{SILVER_BASE}'
""")

# COMMAND ----------

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.dim_medicine (
    medicine_id   STRING,
    medicine_name STRING,
    category      STRING
)
USING DELTA
LOCATION '{SILVER_BASE}/dim_medicine'
""")

# COMMAND ----------

# COMMAND ----------
spark.sql("DROP TABLE IF EXISTS silver.fact_medicine_inventory")
dbutils.fs.rm(f"{SILVER_BASE}/fact_medicine_inventory", True)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.fact_medicine_inventory (
    medicine_id   STRING,
    hospital_id   STRING,
    total_stock   INT,
    issued_stock  INT,
    expiry_date   DATE,
    available_stock INT
)
USING DELTA
LOCATION '{SILVER_BASE}/fact_medicine_inventory'
""")

# COMMAND ----------

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.dim_device (
    device_id   STRING,
    device_type STRING,
    hospital_id STRING
)
USING DELTA
LOCATION '{SILVER_BASE}/dim_device'
""")


# COMMAND ----------

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.fact_device_usage (
    device_id    STRING,
    hospital_id  STRING,
    week         STRING,
    total_units  INT,
    in_use_units INT,
    available_units INT
)
USING DELTA
LOCATION '{SILVER_BASE}/fact_device_usage'
""")

# COMMAND ----------

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.dim_insurance (
    insurance_id   STRING,
    insurance_name STRING
)
USING DELTA
LOCATION '{SILVER_BASE}/dim_insurance'
""")

# COMMAND ----------

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.dim_patient (
    patient_id     STRING,
    patient_gender STRING,
    patient_dob    DATE,
    insurance_id   STRING
)
USING DELTA
LOCATION '{SILVER_BASE}/dim_patient'
""")

# COMMAND ----------

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.dim_doctor (
    doctor_id   STRING,
    department  STRING,
    hospital_id STRING
)
USING DELTA
LOCATION '{SILVER_BASE}/dim_doctor'
""")

# COMMAND ----------

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.dim_bed (
    bed_id      STRING,
    bed_type    STRING,
    hospital_id STRING
)
USING DELTA
LOCATION '{SILVER_BASE}/dim_bed'
""")

# COMMAND ----------

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.fact_admission (
    admission_id         STRING,
    patient_id           STRING,
    doctor_id            STRING,
    bed_id               STRING,
    hospital_id          STRING,
    admission_time       TIMESTAMP,
    discharge_time       TIMESTAMP,
    length_of_stay_hrs   INT,
    is_currently_admitted BOOLEAN
)
USING DELTA
LOCATION '{SILVER_BASE}/fact_admission'
""")

# COMMAND ----------

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.fact_billing (
    admission_id    STRING,
    insurance_id    STRING,
    billed_amount   DOUBLE,
    approved_amount DOUBLE,
    claim_status    STRING
)
USING DELTA
LOCATION '{SILVER_BASE}/fact_billing'
""")

# COMMAND ----------

print("All silver tables created successfully under bronzelayer/silver/")