# Databricks notebook source
# MAGIC %run "/Workspace/Users/avinashanu101@gmail.com/Helper_NB"

# COMMAND ----------

# NOTEBOOK-SPECIFIC CONFIG
# (secrets, ADLS conf, CATALOG, AUDIT_TABLE provided by Helper_NB)

# COMMAND ----------

# DBTITLE 1,Set Parameterized File Paths
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit, to_timestamp

# Based on architecture, the trigger passes report_month natively (e.g. 2026-01)
hospital_raw_path = f"abfss://{raw_container}@{storage_account_name}.dfs.core.windows.net/Raw_Data/Hospital_events_{report_month}.csv"
device_raw_path = f"abfss://{raw_container}@{storage_account_name}.dfs.core.windows.net/Raw_Data/Device_events_{report_month}.csv"
pharma_raw_path = f"abfss://{raw_container}@{storage_account_name}.dfs.core.windows.net/Raw_Data/Pharma_events_{report_month}.csv"

hospital_bronze_path = f"abfss://{bronze_container}@{storage_account_name}.dfs.core.windows.net/Staging_Data/Hospital_Data/"
device_bronze_path = f"abfss://{bronze_container}@{storage_account_name}.dfs.core.windows.net/Staging_Data/Device_Data/"
pharma_bronze_path = f"abfss://{bronze_container}@{storage_account_name}.dfs.core.windows.net/Staging_Data/Pharma_Data/"

# COMMAND ----------

# DBTITLE 1,Explicit Schemas
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

hospital_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("patient_gender", StringType(), True),
    StructField("patient_dob", StringType(), True), # Load as string parse later
    StructField("insurance_id", StringType(), True),
    StructField("insurance_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("doctor_id", StringType(), True),
    StructField("bed_id", StringType(), True),
    StructField("bed_type", StringType(), True),
    StructField("hospital_id", StringType(), False),
    StructField("admission_time", StringType(), True),
    StructField("discharge_time", StringType(), True),
    StructField("billed_amount", StringType(), True),
    StructField("approved_amount", StringType(), True),
    StructField("claim_status", StringType(), True)
])

device_schema = StructType([
    StructField("hospital_id", StringType(), False),
    StructField("device_id", StringType(), False),
    StructField("device_type", StringType(), True),
    StructField("total_units", IntegerType(), True),
    StructField("in_use_units", IntegerType(), True),
    StructField("week", StringType(), True)
])

pharma_schema = StructType([
    StructField("medicine_id", StringType(), False),
    StructField("medicine_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("hospital_id", StringType(), False),
    StructField("total_stock", IntegerType(), True),
    StructField("issued_stock", IntegerType(), True),
    StructField("expiry_date", StringType(), True)
])

# COMMAND ----------

def process_and_enrich_data(path, schema, source_name, primary_keys, bronze_path):
    print(f"Processing {source_name} from {path}")
    
    # 1. Explicit Schema Read (FAILFAST)
    df = spark.read.schema(schema).option("header", True).option("mode", "FAILFAST").csv(path)
    
    # 2. Blank/Null Check on Primary Keys
    df = df.dropna(subset=primary_keys)
    
    # 3. Deduplication on Primary Key
    df = df.dropDuplicates(subset=primary_keys)
    
    # 4. Enforce specific timestamp conversions if relevant
    if "admission_time" in df.columns:
        df = df.withColumn("admission_time", to_timestamp("admission_time"))
        df = df.withColumn("discharge_time", to_timestamp("discharge_time"))

    # 5. Add Metadata Columns
    df = df.withColumn("_ingestion_run_id", lit(run_id)) \
           .withColumn("_report_month", lit(report_month)) \
           .withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source", lit(source_name))
           
    # 6. Partition by _report_month and write to Bronze
    # Mode is set to Append since month ranges arrive weekly; overwrite would destroy older weeks
    df.write.partitionBy("_report_month").mode("append").parquet(bronze_path)
    
    # 7. Write Per-Source Audit Entry natively leveraging the updated Helper_NB methods
    write_audit_record(pipeline_name, notebook_name, run_id, source_name, "raw", df.count(), "Success", report_month)

# COMMAND ----------

# Process Hospital Events
process_and_enrich_data(
    hospital_raw_path,
    hospital_schema,
    "MySQL_Hospital",
    ["event_id"],
    hospital_bronze_path
)

# Process Device Events
process_and_enrich_data(
    device_raw_path,
    device_schema,
    "AzureSQL_Device",
    ["hospital_id", "device_id", "week"],
    device_bronze_path
)

# Process Pharma Events
process_and_enrich_data(
    pharma_raw_path,
    pharma_schema,
    "PostgreSQL_Pharma",
    ["medicine_id", "hospital_id"],
    pharma_bronze_path
)

# COMMAND ----------

dbutils.notebook.exit("All sources validated schemas, deduplicated, partitioned, and written to Bronze.")
