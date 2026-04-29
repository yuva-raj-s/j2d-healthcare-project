# Databricks notebook source
# -------------------------------------------------------------------------
# Create_Tables — Silver schema and tables under Unity Catalog
#
# CATALOG and AUDIT_TABLE are inherited from Helper_NB via %run in Silver_NB.
# All tables are FULLY MANAGED by Unity Catalog.
# Rule: Never use LOCATION in CREATE SCHEMA or CREATE TABLE when using
#       Unity Catalog managed storage — UC handles paths automatically.
# -------------------------------------------------------------------------

# COMMAND ----------

# Drop and recreate silver schema cleanly
spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.silver CASCADE")
print(f"Dropped schema: {CATALOG}.silver")

# COMMAND ----------

# Managed schema — no LOCATION, Unity Catalog controls storage
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
print(f"Created schema: {CATALOG}.silver")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_medicine (
    medicine_id   STRING,
    medicine_name STRING,
    category      STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.fact_medicine_inventory (
    medicine_id     STRING,
    hospital_id     STRING,
    total_stock     INT,
    issued_stock    INT,
    expiry_date     DATE,
    available_stock INT
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_device (
    device_id   STRING,
    device_type STRING,
    hospital_id STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.fact_device_usage (
    device_id       STRING,
    hospital_id     STRING,
    week            STRING,
    total_units     INT,
    in_use_units    INT,
    available_units INT
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_insurance (
    insurance_id   STRING,
    insurance_name STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_patient (
    patient_id     STRING,
    patient_gender STRING,
    patient_dob    DATE,
    insurance_id   STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_doctor (
    doctor_id   STRING,
    department  STRING,
    hospital_id STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_bed (
    bed_id      STRING,
    bed_type    STRING,
    hospital_id STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.fact_admission (
    admission_id          STRING,
    patient_id            STRING,
    doctor_id             STRING,
    bed_id                STRING,
    hospital_id           STRING,
    admission_time        TIMESTAMP,
    discharge_time        TIMESTAMP,
    length_of_stay_hrs    INT,
    is_currently_admitted BOOLEAN
)
USING DELTA
""")

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
""")

# COMMAND ----------

print(f"All silver tables created successfully under {CATALOG}.silver")
spark.sql(f"SHOW TABLES IN {CATALOG}.silver").show(truncate=False)