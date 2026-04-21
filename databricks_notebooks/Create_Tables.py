# Databricks notebook source
# -------------------------------------------------------------------------
# Unity Catalog Configuration
# All objects live under: j2d_databricks_04.silver.<table>
# -------------------------------------------------------------------------
CATALOG = "j2d_databricks_04"

# Drop the stale silver schema entirely from the Unity Catalog
spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.silver CASCADE")

print("Cleanup done")

# COMMAND ----------

# Databricks notebook source

# Define base path once — easy to change later
SILVER_BASE = "abfss://bronzelayer@j2dstorage101.dfs.core.windows.net/silver"

# COMMAND ----------

# COMMAND ----------
# Create schema pointing to bronzelayer under the Unity Catalog
# In Unity Catalog, LOCATION is set via external location — the schema
# is created under the catalog and storage is managed by the catalog.
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver
    MANAGED LOCATION '{SILVER_BASE}'
""")

# COMMAND ----------

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_medicine (
    medicine_id   STRING,
    medicine_name STRING,
    category      STRING
)
USING DELTA
LOCATION '{SILVER_BASE}/dim_medicine'
""")

# COMMAND ----------

# COMMAND ----------
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.silver.fact_medicine_inventory")
dbutils.fs.rm(f"{SILVER_BASE}/fact_medicine_inventory", True)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.fact_medicine_inventory (
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
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_device (
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
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.fact_device_usage (
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
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_insurance (
    insurance_id   STRING,
    insurance_name STRING
)
USING DELTA
LOCATION '{SILVER_BASE}/dim_insurance'
""")

# COMMAND ----------

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_patient (
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
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_doctor (
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
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_bed (
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
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.fact_admission (
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
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.fact_billing (
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

print(f"All silver tables created successfully under {CATALOG}.silver in Unity Catalog.")