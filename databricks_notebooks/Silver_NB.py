# Databricks notebook source
# MAGIC %run "/Workspace/Users/avinashanu101@gmail.com/Helper_NB"

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
storage_account_name = "j2dstorage101"
container_name = "bronzelayer"

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

from datetime import datetime
from dateutil.relativedelta import relativedelta
parsed_date = datetime.strptime(input_date, "%Y%m%d")
prev_month_date = parsed_date - relativedelta(months=1)
prev_month_full_name = prev_month_date.strftime("%B")
print(parsed_date)
print(prev_month_date)
print(prev_month_full_name)

# COMMAND ----------

hospital_bronze_path=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Staging_Data/Hospital_Data/"
device_bronze_path=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Staging_Data/Device_Data/"
pharma_bronze_path=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Staging_Data/Pharma_Data/"

# COMMAND ----------

hospital_df = spark.read.parquet(hospital_bronze_path)
display(hospital_df)

# COMMAND ----------

# DBTITLE 1,Data Cleansing and Validation for Silver Layer Records
"""
Cleansing and Standardization in silver
1. Standardize the date format
2. Filtering the gender values other than M,F,O 
3. Dob should be lesser then current date
4. Filter Hospital Data Based on Admission and Discharge Time
5. billed_amount should be greater than or equal to approved_amount
"""

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp
#Standardize the date format
silver_clean_hospital_df = (
    hospital_df
    .withColumn("admission_time", to_timestamp("admission_time"))
    .withColumn("discharge_time", to_timestamp("discharge_time"))
)


# COMMAND ----------

#Filtering the gender values other than M,F,O 
#it differs from bronze layer validation where we are checking for male female shold no be there 

silver_clean_hospital_df = (
    silver_clean_hospital_df
    .filter(col("patient_gender").isin("M", "F", "O"))
)

# COMMAND ----------

# DBTITLE 1,Filter Patients by Date of Birth in Hospital Data

#Dob should be lesser then current date
silver_clean_hospital_df = (
    silver_clean_hospital_df
    .filter(col("patient_dob") <= current_date())
)



# COMMAND ----------

# DBTITLE 1,Filter Hospital Data Based on Admission and Discharge Time
#Filter Hospital Data Based on Admission and Discharge Time
silver_clean_hospital_df = (
    silver_clean_hospital_df
    .filter(
        col("discharge_time").isNull() |
        (col("admission_time") <= col("discharge_time"))
    )
)



# COMMAND ----------

# DBTITLE 1,Filter Hospitals with Billed Amounts Exceeding Approved ...
silver_clean_hospital_df = (
    silver_clean_hospital_df
    .filter(col("billed_amount") >= col("approved_amount"))
)


# COMMAND ----------

display(silver_clean_hospital_df)

# COMMAND ----------

pharmacy_df = spark.read.parquet(pharma_bronze_path)
display(pharmacy_df)

# COMMAND ----------

"""
Adding new colums for downstream calculations
1.available_stock
2.expiry_status
"""

# COMMAND ----------

from pyspark.sql.functions import col, to_date

pharmacy_df = pharmacy_df.withColumn(
    "expiry_date",
    to_date(col("expiry_date"))
).withColumn(
    "total_stock",
    col("total_stock").cast("int")
).withColumn(
    "issued_stock",
    col("issued_stock").cast("int")
)
display(pharmacy_df)

# COMMAND ----------

pharmacy_df = pharmacy_df.withColumn(
    "available_stock",
    col("total_stock").cast('int') - col("issued_stock").cast('int')
)
display(pharmacy_df)

# COMMAND ----------

from pyspark.sql.functions import when
pharmacy_df = pharmacy_df.withColumn(
    "expiry_status",
    when(col("expiry_date") < current_date(), "EXPIRED")
    .otherwise("VALID")
)

# COMMAND ----------

validated_pharmacy_df = pharmacy_df.filter(
    (col("medicine_id").isNotNull()) &
    (col("hospital_id").isNotNull()) &
    (col("total_stock") >= 0) &
    (col("issued_stock") >= 0) &
    (col("issued_stock") <= col("total_stock")) &
    (col("expiry_status") == "VALID")
)



# COMMAND ----------

display(validated_pharmacy_df)

# COMMAND ----------

device_df = spark.read.parquet(device_bronze_path)
display(device_df)

# COMMAND ----------

dim_medicine_df = (
    validated_pharmacy_df
    .select("medicine_id", "medicine_name", "category")
    .dropDuplicates()
)
display(dim_medicine_df)

# COMMAND ----------

# Delete and insert using Spark SQL
spark.sql("DELETE FROM silver.dim_medicine")
dim_medicine_df.createOrReplaceTempView("tmp_dim_medicine")
spark.sql("""
INSERT INTO silver.dim_medicine
SELECT * FROM tmp_dim_medicine
""")

display(spark.read.table("silver.dim_medicine"))

# COMMAND ----------

from pyspark.sql.functions import col

fact_medicine_inventory_df = (
    validated_pharmacy_df
    .select(
        "medicine_id",
        "hospital_id",
        "total_stock",
        "issued_stock",
        "expiry_date"
    )
    .withColumn(
        "available_stock",
        col("total_stock") - col("issued_stock")
    )
)




# COMMAND ----------

# Delete all records from the table
spark.sql("DELETE FROM silver.fact_medicine_inventory")

# Register DataFrame as temp view for insert
fact_medicine_inventory_df.createOrReplaceTempView("tmp_fact_medicine_inventory")

# Insert data into the table
spark.sql("""
INSERT INTO silver.fact_medicine_inventory
SELECT * FROM tmp_fact_medicine_inventory
""")

display(spark.read.table("silver.fact_medicine_inventory"))

# COMMAND ----------

dim_device_df = (
    device_df
    .select("device_id", "device_type", "hospital_id")
    .dropDuplicates()
)

# COMMAND ----------



# Delete all records from the table
spark.sql("DELETE FROM silver.dim_device")

# Register DataFrame as temp view for insert
dim_device_df.createOrReplaceTempView("tmp_dim_device")

# Insert data into the table
spark.sql("""
INSERT INTO silver.dim_device
SELECT * FROM tmp_dim_device
""")

display(spark.read.table("silver.dim_device"))

# COMMAND ----------

fact_device_usage_df = (
    device_df
    .select(
        "device_id",
        "hospital_id",
        "week",
        "total_units",
        "in_use_units"
    )
    .withColumn(
        "available_units",
        col("total_units").cast("int") - col("in_use_units").cast("int")
    )
)

# COMMAND ----------

# Delete all records from the table
spark.sql("DELETE FROM silver.fact_device_usage")

# Register DataFrame as temp view for insert
fact_device_usage_df.createOrReplaceTempView("tmp_fact_device_usage")

# Insert data into the table
spark.sql("""
INSERT INTO silver.fact_device_usage
SELECT * FROM tmp_fact_device_usage
""")

display(spark.read.table("silver.fact_device_usage"))

# COMMAND ----------

dim_insurance_df = (
    silver_clean_hospital_df
    .select("insurance_id","insurance_name")
    .dropDuplicates()
)
display(dim_insurance_df)

# COMMAND ----------

# Delete all records from the table
spark.sql("DELETE FROM silver.dim_insurance")

# Register DataFrame as temp view for insert
dim_insurance_df.createOrReplaceTempView("tmp_dim_insurance")

# Insert data into the table
spark.sql("""
INSERT INTO silver.dim_insurance
SELECT * FROM tmp_dim_insurance
""")

display(spark.read.table("silver.dim_insurance"))

# COMMAND ----------

dim_patient_df = (
    silver_clean_hospital_df
    .select("patient_id", "patient_gender", "patient_dob", "insurance_id")
    .dropDuplicates()
)
display(dim_patient_df)

# COMMAND ----------

# Delete all records from the table
spark.sql("DELETE FROM silver.dim_patient")

# Register DataFrame as temp view for insert
dim_patient_df.createOrReplaceTempView("tmp_dim_patient")

# Insert data into the table
spark.sql("""
INSERT INTO silver.dim_patient
SELECT * FROM tmp_dim_patient
""")

display(spark.read.table("silver.dim_patient"))

# COMMAND ----------

dim_doctor_df = (
    silver_clean_hospital_df
    .select("doctor_id", "department", "hospital_id")
    .dropDuplicates()
)
display(dim_doctor_df)

# COMMAND ----------

# Delete all records from the table
spark.sql("DELETE FROM silver.dim_doctor")

# Register DataFrame as temp view for insert
dim_doctor_df.createOrReplaceTempView("tmp_dim_doctor")

# Insert data into the table
spark.sql("""
INSERT INTO silver.dim_doctor
SELECT * FROM tmp_dim_doctor
""")

display(spark.read.table("silver.dim_doctor"))

# COMMAND ----------

dim_bed_df = (
    silver_clean_hospital_df
    .select("bed_id", "bed_type", "hospital_id")
    .dropDuplicates()
)
display(dim_bed_df)

# COMMAND ----------

# Delete all records from the table
spark.sql("DELETE FROM silver.dim_bed")

# Register DataFrame as temp view for insert
dim_bed_df.createOrReplaceTempView("tmp_dim_bed")

# Insert data into the table
spark.sql("""
INSERT INTO silver.dim_bed
SELECT * FROM tmp_dim_bed
""")

display(spark.read.table("silver.dim_bed"))

# COMMAND ----------

from pyspark.sql.functions import when, datediff

fact_admission_df = (
    silver_clean_hospital_df
    .select(
        col("event_id").alias("admission_id"),
        "patient_id",
        "doctor_id",
        "bed_id",
        "hospital_id",
        "admission_time",
        "discharge_time",
        when(
            col("discharge_time").isNotNull(),
            datediff(col("discharge_time"), col("admission_time")) * 24
        ).alias("length_of_stay_hrs"),
        col("discharge_time").isNull().alias("is_currently_admitted")
    )
)
display(fact_admission_df)

# COMMAND ----------

# Delete all records from the table
spark.sql("DELETE FROM silver.fact_admission")

# Register DataFrame as temp view for insert
fact_admission_df.createOrReplaceTempView("tmp_fact_admission")

# Insert data into the table
spark.sql("""
INSERT INTO silver.fact_admission
SELECT * FROM tmp_fact_admission
""")



# COMMAND ----------

fact_billing_df = (
    silver_clean_hospital_df
    .select(
        col("event_id").alias("admission_id"),
        "insurance_id",
        "billed_amount",
        "approved_amount",
        "claim_status"
    )
)
display(fact_billing_df)

# COMMAND ----------



# Delete all records from the table
spark.sql("DELETE FROM silver.fact_billing")

# Register DataFrame as temp view for insert
fact_billing_df.createOrReplaceTempView("tmp_fact_billing")

# Insert data into the table
spark.sql("""
INSERT INTO silver.fact_billing
SELECT * FROM tmp_fact_billing
""")

display(spark.read.table("silver.fact_billing"))

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

from datetime import date
write_audit_record(silver_clean_hospital_df, "silver", prev_month_full_name)
write_audit_record(device_df,               "silver", prev_month_full_name)
write_audit_record(validated_pharmacy_df,   "silver", prev_month_full_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from J2D_Audit_table where notebook_name='Silver_NB'

# COMMAND ----------

success='All validations are successfully passed'
dbutils.notebook.exit(success)