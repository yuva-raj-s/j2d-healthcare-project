# Databricks notebook source
# MAGIC %run "/Workspace/Users/kattayashu2020@gmail.com/Helper_NB"

# COMMAND ----------

# -------------------------
# CONFIGURATION
# -------------------------

# Retrieve secrets from Key Vault (Scope: adls)
mysql_password = dbutils.secrets.get(scope="adls", key="mysql-password")
azuresql_password = dbutils.secrets.get(scope="adls", key="azuresql-password")
postgresql_password = dbutils.secrets.get(scope="adls", key="postgresql-password")
adls_account_key = dbutils.secrets.get(scope="adls", key="adls-account-key")
synapse_password = dbutils.secrets.get(scope="adls", key="synapse-password")

# Replace with your values
storage_account_name = "j2dstorage04"
container_name = "rawlayer"

# -------------------------
# SETUP CONFIGURATION
# -------------------------
spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  adls_account_key
)

# COMMAND ----------

dbutils.widgets.text("pipeline_name", "")
dbutils.widgets.text("notebook_name", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("source", "")
dbutils.widgets.text("input_date", "")


pipeline_name = dbutils.widgets.get("pipeline_name")
notebook_name = dbutils.widgets.get("notebook_name")
run_id = dbutils.widgets.get("run_id")
source = dbutils.widgets.get("source")
input_date = dbutils.widgets.get("input_date")

# COMMAND ----------



parsed_date = datetime.strptime(input_date, "%Y%m%d")
prev_month_date = parsed_date - relativedelta(months=1)
prev_month_full_name = prev_month_date.strftime("%B")
print(parsed_date)
print(prev_month_date)
print(prev_month_full_name)

# COMMAND ----------

display(dbutils.fs.ls("abfss://rawlayer@j2dstorage04.dfs.core.windows.net/Raw_Data/"))

# COMMAND ----------

"""
Three source files to process
1.Hospital Data
2.Device Data
3.Pharma Data
"""

# COMMAND ----------

# DBTITLE 1,Set File Paths for January 2026 Raw Event Data
hospital_raw_path='abfss://rawlayer@j2dstorage04.dfs.core.windows.net/Raw_Data/Hospital_events_January_2026.csv'
device_raw_path='abfss://rawlayer@j2dstorage04.dfs.core.windows.net/Raw_Data/Device_events_January_2026.csv'
pharma_raw_path='abfss://rawlayer@j2dstorage04.dfs.core.windows.net/Raw_Data/Pharma_events_January_2026.csv'

# COMMAND ----------

# DBTITLE 1,Define Storage Paths for Hospital Device and Pharma Dat ...
hospital_bronze_path='abfss://bronzelayer@j2dstorage04.dfs.core.windows.net/Staging_Data/Hospital_Data/'
device_bronze_path='abfss://bronzelayer@j2dstorage04.dfs.core.windows.net/Staging_Data/Device_Data/'
pharma_bronze_path='abfss://bronzelayer@j2dstorage04.dfs.core.windows.net/Staging_Data/Pharma_Data/'

# COMMAND ----------

# DBTITLE 1,Define CSV Reader Function Adding File Path Column
from pyspark.sql.functions import input_file_name,lit

def read_csv_file(file_path):
    return (
        spark.read.option("header", True).csv(file_path)
        .withColumn("file_path", lit(file_path))
    )


# COMMAND ----------

# DBTITLE 1,Load and Display Raw Hospital Data for Analysis
hospital_raw_df = read_csv_file(hospital_raw_path)
display(hospital_raw_df)

# COMMAND ----------

"""
Basic checks in Bronze layer

1.Schema Validation (Incoming columns has to match with expected Schema)
2.Null Check
3.String Length Check
4.Integer Check
"""

# COMMAND ----------

# DBTITLE 1,Define Expected Hospital Dataset Column Names
hospital_raw_columns_expected=['event_id',
 'patient_id',
 'patient_gender',
 'patient_dob',
 'insurance_id',
 'insurance_name',
 'department',
 'doctor_id',
 'bed_id',
 'bed_type',
 'hospital_id',
 'admission_time',
 'discharge_time',
 'billed_amount',
 'approved_amount',
 'claim_status',
 'file_path']

# COMMAND ----------

# DBTITLE 1,Check Hospital Raw Data Columns for Schema Mismatch
#Check Hospital Raw Data Columns for Schema Mismatch
if hospital_raw_columns_expected != hospital_raw_df.columns:
    dbutils.notebook.exit("Column mismatch: hospital_raw_columns_expected does not match raw_df.columns")

# COMMAND ----------

# DBTITLE 1,Extract Month and Year from Hospital File Names
from pyspark.sql.functions import split, col, regexp_replace
hospital_new_df = hospital_raw_df.withColumn(
    "file_name_last", split("file_path", "/")[4]
).withColumn(
    "Month", split(col("file_name_last"),'_')[2]
).withColumn(
    "Year", regexp_replace(split(col("file_name_last"),'_')[3], r"\.csv", "")
)
display(hospital_new_df)

# COMMAND ----------

# DBTITLE 1,Identify Key Columns for Hospital Data Integrity Check
hospital_null_checkist=['insurance_id',
 'billed_amount',
 'approved_amount',
 'patient_dob'
 ]

# COMMAND ----------

# DBTITLE 1,Evaluate Null Values in Hospital Dataset Columns
from pyspark.sql.functions import col, sum

null_counts_row = (
    hospital_new_df
    .select([col(c).isNull().alias(c) for c in hospital_null_checkist])
    .agg(*[sum(col(c).cast("int")).alias(c) for c in hospital_null_checkist])
    .collect()[0]
)
null_counts = {c: null_counts_row[c] for c in hospital_null_checkist}
display(null_counts)
if any(null_counts[c] > 0 for c in hospital_null_checkist):
    dbutils.notebook.exit("Null is there please check the data")

# COMMAND ----------

# DBTITLE 1,Validate Patient Gender String Length for Hospital Data
#To confirm the length of the string is not above than 1 ..avoiding male and female 
# M and F are allowed
check_string_length(hospital_new_df, 'patient_gender', 1) 

# COMMAND ----------

# DBTITLE 1,Verify Integer Type for Hospital Amount Columns
check_column_is_int(hospital_new_df,'billed_amount')
check_column_is_int(hospital_new_df,'approved_amount') 

# COMMAND ----------

hospital_new_df.coalesce(1).write.option("header", True).format("parquet").mode("overwrite").save(hospital_bronze_path)

# COMMAND ----------

# DBTITLE 1,Load and Display Device Raw Data from CSV File
device_raw_df = read_csv_file(device_raw_path)
display(device_raw_df)

# COMMAND ----------

# DBTITLE 1,List of Columns in Device Raw Dataframe
device_raw_df.columns

# COMMAND ----------

device_expected_columns=['hospital_id',
 'device_id',
 'device_type',
 'total_units',
 'in_use_units',
 'week',
 'file_path']

# COMMAND ----------

# Check Device Raw Data Columns for Schema Mismatch
if device_expected_columns != device_raw_df.columns:
    dbutils.notebook.exit("Column mismatch: device_expected_columns does not match device_raw_df.columns")

# COMMAND ----------

from pyspark.sql.functions import split, col, regexp_replace
device_new_df = device_raw_df.withColumn(
    "file_name_last", split("file_path", "/")[4]
).withColumn(
    "Month", split(col("file_name_last"),'_')[2]
).withColumn(
    "Year", regexp_replace(split(col("file_name_last"),'_')[3], r"\.csv", "")
)
display(device_new_df)

# COMMAND ----------

check_string_length(device_new_df, 'device_id', 6)

# COMMAND ----------

check_column_is_int(device_new_df,'total_units')
check_column_is_int(device_new_df,'in_use_units') 

# COMMAND ----------

from pyspark.sql.functions import col, sum
device_null_checkist=['hospital_id',
 'device_id'
 ]

null_counts_row = (
    device_new_df
    .select([col(c).isNull().alias(c) for c in device_null_checkist])
    .agg(*[sum(col(c).cast("int")).alias(c) for c in device_null_checkist])
    .collect()[0]
)
null_counts = {c: null_counts_row[c] for c in device_null_checkist}
display(null_counts)
if any(null_counts[c] > 0 for c in device_null_checkist):
    dbutils.notebook.exit("Null is there please check the data")

# COMMAND ----------

device_new_df.coalesce(1).write.option("header", True).format("parquet").mode("overwrite").save(device_bronze_path)

# COMMAND ----------

pharma_raw_df = read_csv_file(pharma_raw_path)
display(pharma_raw_df)

# COMMAND ----------

pharma_raw_df.columns

# COMMAND ----------

pharma_expected_columns=['medicine_id',
 'medicine_name',
 'category',
 'hospital_id',
 'total_stock',
 'issued_stock',
 'expiry_date',
 'file_path']

# COMMAND ----------

# Check Pharma Raw Data Columns for Schema Mismatch
if pharma_expected_columns != pharma_raw_df.columns:
    dbutils.notebook.exit("Column mismatch: pharma_expected_columns does not match pharma_raw_df.columns")

# COMMAND ----------

from pyspark.sql.functions import split, col, regexp_replace
pharmacy_df = pharma_raw_df.withColumn(
    "file_name_last", split("file_path", "/")[4]
).withColumn(
    "Month", split(col("file_name_last"),'_')[2]
).withColumn(
    "Year", regexp_replace(split(col("file_name_last"),'_')[3], r"\.csv", "")
)
display(pharmacy_df)

# COMMAND ----------

from pyspark.sql.functions import col, sum
pharma_null_checkist=['hospital_id',
 'medicine_id'
 ]

null_counts_row = (
    pharmacy_df
    .select([col(c).isNull().alias(c) for c in pharma_null_checkist])
    .agg(*[sum(col(c).cast("int")).alias(c) for c in pharma_null_checkist])
    .collect()[0]
)
null_counts = {c: null_counts_row[c] for c in pharma_null_checkist}
display(null_counts)
if any(null_counts[c] > 0 for c in pharma_null_checkist):
    dbutils.notebook.exit("Null is there please check the data")

# COMMAND ----------

check_string_length(pharmacy_df, 'medicine_id', 6)

# COMMAND ----------

check_column_is_int(pharmacy_df,'total_stock')
check_column_is_int(pharmacy_df,'issued_stock') 

# COMMAND ----------

pharmacy_df.coalesce(1).write.option("header", True).format("parquet").mode("overwrite").save(pharma_bronze_path)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

audit_schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("notebook_name", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("record_count", IntegerType(), True),
    StructField("load_time", DateType(), True),
    StructField("status", StringType(), True),
    StructField("report_month", StringType(), True)
])

from pyspark.sql.functions import lit, current_date

def write_audit_record(df, layer, report_month):
    audit_df = spark.createDataFrame(
        [(pipeline_name, notebook_name, run_id, source, layer, df.count(), date.today(), "Success", report_month)],
        schema=audit_schema
    )
    audit_df.write.mode("append").saveAsTable("J2D_Audit_table")

# COMMAND ----------

from datetime import date  # ensure date is available before audit writes

write_audit_record(hospital_new_df, "raw", prev_month_full_name)
write_audit_record(device_new_df,   "raw", prev_month_full_name)
write_audit_record(pharmacy_df,     "raw", prev_month_full_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from J2D_Audit_table where notebook_name='raw_nb'

# COMMAND ----------

success='All validations are successfully passed'
dbutils.notebook.exit(success)