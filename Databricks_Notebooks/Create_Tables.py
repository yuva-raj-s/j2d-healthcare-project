# Databricks notebook source
# Create table
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.dim_medicine (
    medicine_id STRING,
    medicine_name STRING,
    category STRING
)
USING DELTA
""")

# COMMAND ----------

# Create table if not exists
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.fact_medicine_inventory (
    medicine_id STRING,
    hospital_id STRING,
    total_stock INT,
    issued_stock INT,
    expiry_date DATE,
    available_stock INT
)
USING DELTA
""")

# COMMAND ----------

# Create table
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.dim_device (
    device_id STRING,
    device_type STRING,
    hospital_id STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.fact_device_usage (
    device_id STRING,
    hospital_id STRING,
    week STRING,
    total_units INT,
    in_use_units INT,
    available_units INT
)
USING DELTA
""")

# COMMAND ----------

# Create table if not exists
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.dim_insurance (
    insurance_id STRING,
    insurance_name STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.dim_patient (
    patient_id STRING,
    patient_gender STRING,
    patient_dob DATE,
    insurance_id STRING
) USING DELTA
""")

# COMMAND ----------

# Create table
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.dim_doctor (
    doctor_id STRING,
    department STRING,
    hospital_id STRING
)
USING DELTA
""")

# COMMAND ----------

# Create table
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.dim_bed (
    bed_id STRING,
    bed_type STRING,
    hospital_id STRING
)
USING DELTA
""")

# COMMAND ----------

#Spark SQL syntax for CREATE TABLE
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.fact_admission (
    admission_id STRING,
    patient_id STRING,
    doctor_id STRING,
    bed_id STRING,
    hospital_id STRING,
    admission_time TIMESTAMP,
    discharge_time TIMESTAMP,
    length_of_stay_hrs INT,
    is_currently_admitted BOOLEAN
)
USING DELTA
""")

# COMMAND ----------

# Create table if not exists
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.fact_billing (
    admission_id    STRING,
    insurance_id    STRING,
    billed_amount   DOUBLE,
    approved_amount DOUBLE,
    claim_status    STRING
)
USING DELTA
""")