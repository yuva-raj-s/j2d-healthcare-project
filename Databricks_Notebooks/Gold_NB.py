# Databricks notebook source
# MAGIC %run "/Workspace/Users/kattayashu2020@gmail.com/Helper_NB"

# COMMAND ----------

# -------------------------
# CONFIGURATION
# -------------------------

# Replace with your values (same as Raw/Silver NB)
storage_account_name = "j2dstorage04"
account_key = "<REDACTED_AZURE_KEY>"

# Secure option: Store the key in Databricks secrets instead of hardcoding
# account_key = dbutils.secrets.get(scope="my-scope", key="storage-account-key")

# -------------------------
# SETUP CONFIGURATION
# -------------------------
spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  account_key
)

# COMMAND ----------

# -------------------------
# WIDGETS / PARAMETERS
# -------------------------
dbutils.widgets.text("pipeline_name", "")
dbutils.widgets.text("notebook_name", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("source", "")
dbutils.widgets.text("input_date", "")

pipeline_name = dbutils.widgets.get("pipeline_name")
notebook_name = dbutils.widgets.get("notebook_name")
run_id        = dbutils.widgets.get("run_id")
source        = dbutils.widgets.get("source")
input_date    = dbutils.widgets.get("input_date")

# COMMAND ----------

# -------------------------
# DATE DERIVATION
# -------------------------
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

parsed_date         = datetime.strptime(input_date, "%Y%m%d")
prev_month_date     = parsed_date - relativedelta(months=1)
prev_month_full_name = prev_month_date.strftime("%B")
prev_month_year     = prev_month_date.strftime("%Y")

print(f"Processing for month : {prev_month_full_name} {prev_month_year}")

# COMMAND ----------

# -------------------------
# GOLD LAYER OUTPUT PATHS
# -------------------------
gold_base_path = f"abfss://goldlayer@{storage_account_name}.dfs.core.windows.net/Gold_Data"

gold_medicine_availability_path = f"{gold_base_path}/gold_medicine_availability/"
gold_device_availability_path   = f"{gold_base_path}/gold_device_availability/"
gold_finance_weekly_path        = f"{gold_base_path}/gold_finance_weekly/"
gold_weekly_admissions_path     = f"{gold_base_path}/gold_weekly_admissions/"
gold_claim_status_summary_path  = f"{gold_base_path}/gold_claim_status_summary/"
gold_rejected_claims_path       = f"{gold_base_path}/gold_rejected_claims/"
gold_patient_status_path        = f"{gold_base_path}/gold_patient_status/"

print("Gold paths configured:")
for path in [
    gold_medicine_availability_path,
    gold_device_availability_path,
    gold_finance_weekly_path,
    gold_weekly_admissions_path,
    gold_claim_status_summary_path,
    gold_rejected_claims_path,
    gold_patient_status_path
]:
    print(f"  {path}")

# COMMAND ----------

# -------------------------
# READ FROM SILVER LAYER
# -------------------------

# Unity Catalog qualified table names: <catalog>.<schema>.<table>
CATALOG = "j2d_databricks_04"

fact_medicine_inventory_df = spark.table(f"{CATALOG}.silver.fact_medicine_inventory")
dim_medicine_df            = spark.table(f"{CATALOG}.silver.dim_medicine")
fact_device_usage_df       = spark.table(f"{CATALOG}.silver.fact_device_usage")
dim_device_df              = spark.table(f"{CATALOG}.silver.dim_device")
fact_billing_df            = spark.table(f"{CATALOG}.silver.fact_billing")
fact_admission_df          = spark.table(f"{CATALOG}.silver.fact_admission")
dim_insurance_df           = spark.table(f"{CATALOG}.silver.dim_insurance")

print("All Silver tables loaded successfully.")

# COMMAND ----------

# =========================================================
# GOLD TABLE 1: Medicine Availability by Hospital
# Business Q: Which hospital has low stock of which medicine?
# =========================================================

from pyspark.sql.functions import col, lit

gold_medicine_availability_df = (
    fact_medicine_inventory_df.alias("f")
    .join(dim_medicine_df.alias("d"), "medicine_id", "inner")
    .select(
        "f.hospital_id",
        "d.medicine_name",
        "d.category",
        col("f.available_stock").cast("int"),
        "f.expiry_date",
        lit(prev_month_full_name).alias("report_month")
    )
)

display(gold_medicine_availability_df)

# COMMAND ----------

# Write Gold Table 1 to ADLS as Parquet
gold_medicine_availability_df \
    .coalesce(1) \
    .write \
    .format("parquet") \
    .mode("overwrite") \
    .save(gold_medicine_availability_path)

print(f"[SUCCESS] gold_medicine_availability written to: {gold_medicine_availability_path}")

# COMMAND ----------

# =========================================================
# GOLD TABLE 2: Category-Level Stock Distribution
# Business Q: Which medicine category consumes most stock?
# =========================================================

from pyspark.sql.functions import sum as spark_sum

category_distribution_df = (
    gold_medicine_availability_df
    .groupBy("category")
    .agg(
        spark_sum("available_stock").alias("total_available_stock")
    )
    .withColumn("report_month", lit(prev_month_full_name))
)

display(category_distribution_df)

# COMMAND ----------

# =========================================================
# GOLD TABLE 3: Device Availability by Hospital & Type
# Business Q: Are ICU devices undersupplied in any hospital?
# =========================================================

gold_device_availability_df = (
    fact_device_usage_df.alias("f")
    .join(dim_device_df.alias("d"), ["device_id", "hospital_id"], "inner")
    .groupBy("f.hospital_id", "d.device_type")
    .agg(
        spark_sum("f.total_units").alias("total_units"),
        spark_sum("f.in_use_units").alias("in_use_units"),
        spark_sum("f.available_units").alias("available_units")
    )
    .withColumn(
        "utilization_pct",
        (col("in_use_units") / col("total_units") * 100).cast("double")
    )
    .withColumn("report_month", lit(prev_month_full_name))
)

display(gold_device_availability_df)

# COMMAND ----------

# Write Gold Table 3 to ADLS as Parquet
gold_device_availability_df \
    .coalesce(1) \
    .write \
    .format("parquet") \
    .mode("overwrite") \
    .save(gold_device_availability_path)

print(f"[SUCCESS] gold_device_availability written to: {gold_device_availability_path}")

# COMMAND ----------

# =========================================================
# GOLD TABLE 4: Weekly Finance Summary by Insurer
# Business Q: Revenue leakage, approval %, weekly claims
# =========================================================

from pyspark.sql.functions import date_trunc, count, round as spark_round

finance_base_df = (
    fact_billing_df.alias("fb")
    .join(fact_admission_df.alias("fa"),
          col("fb.admission_id") == col("fa.admission_id"), "inner")
    .join(dim_insurance_df.alias("di"),
          col("fb.insurance_id") == col("di.insurance_id"), "left")
    .filter(col("fb.claim_status") == "APPROVED")
)

finance_week_df = (
    finance_base_df
    .withColumn("week_start_date", date_trunc("week", col("fa.admission_time")))
)

gold_finance_weekly_df = (
    finance_week_df
    .groupBy("week_start_date", "fb.insurance_id", "di.insurance_name")
    .agg(
        count("*").alias("total_claims"),
        spark_sum("fb.billed_amount").alias("total_billed_amount"),
        spark_sum("fb.approved_amount").alias("total_approved_amount"),
        spark_sum(col("fb.billed_amount") - col("fb.approved_amount"))
            .alias("revenue_leakage_amount")
    )
    .withColumn(
        "approval_percentage",
        spark_round(
            (col("total_approved_amount") / col("total_billed_amount")) * 100, 2
        )
    )
    .withColumn("report_month", lit(prev_month_full_name))
)

display(gold_finance_weekly_df)

# COMMAND ----------

# Write Gold Table 4 to ADLS as Parquet
gold_finance_weekly_df \
    .coalesce(1) \
    .write \
    .format("parquet") \
    .mode("overwrite") \
    .save(gold_finance_weekly_path)

print(f"[SUCCESS] gold_finance_weekly written to: {gold_finance_weekly_path}")

# COMMAND ----------

# =========================================================
# GOLD TABLE 5: Weekly Admissions Trend
# Business Q: How many patients were admitted each week?
# =========================================================

gold_weekly_admissions_df = (
    fact_admission_df
    .withColumn("week_start_date", date_trunc("week", col("admission_time")))
    .groupBy("week_start_date")
    .agg(
        count("admission_id").alias("total_admissions")
    )
    .orderBy("week_start_date")
    .withColumn("report_month", lit(prev_month_full_name))
)

display(gold_weekly_admissions_df)

# COMMAND ----------

# Write Gold Table 5 to ADLS as Parquet
gold_weekly_admissions_df \
    .coalesce(1) \
    .write \
    .format("parquet") \
    .mode("overwrite") \
    .save(gold_weekly_admissions_path)

print(f"[SUCCESS] gold_weekly_admissions written to: {gold_weekly_admissions_path}")

# COMMAND ----------

# =========================================================
# GOLD TABLE 6: Claim Status Summary by Insurer
# Business Q: What is the APPROVED / REJECTED / PENDING split per insurer?
# =========================================================

from pyspark.sql.functions import when

claim_status_base_df = (
    fact_billing_df.alias("fb")
    .join(dim_insurance_df.alias("di"),
          col("fb.insurance_id") == col("di.insurance_id"), "left")
)

gold_claim_status_summary_df = (
    claim_status_base_df
    .groupBy("di.insurance_name", "fb.claim_status")
    .agg(
        count("fb.admission_id").alias("claim_count")
    )
    .withColumn("report_month", lit(prev_month_full_name))
)

display(gold_claim_status_summary_df)

# COMMAND ----------

# Write Gold Table 6 to ADLS as Parquet
gold_claim_status_summary_df \
    .coalesce(1) \
    .write \
    .format("parquet") \
    .mode("overwrite") \
    .save(gold_claim_status_summary_path)

print(f"[SUCCESS] gold_claim_status_summary written to: {gold_claim_status_summary_path}")

# COMMAND ----------

# =========================================================
# GOLD TABLE 7: Rejected Claims Ranking by Insurer
# Business Q: Which insurer has highest rejection rate?
# =========================================================

gold_rejected_claims_df = (
    fact_billing_df.alias("fb")
    .filter(col("fb.claim_status") == "REJECTED")
    .join(dim_insurance_df.alias("di"),
          col("fb.insurance_id") == col("di.insurance_id"), "left")
    .groupBy("di.insurance_name")
    .agg(
        count("fb.admission_id").alias("rejected_claim_count")
    )
    .orderBy(col("rejected_claim_count").desc())
    .withColumn("report_month", lit(prev_month_full_name))
)

display(gold_rejected_claims_df)

# COMMAND ----------

# Write Gold Table 7 to ADLS as Parquet
gold_rejected_claims_df \
    .coalesce(1) \
    .write \
    .format("parquet") \
    .mode("overwrite") \
    .save(gold_rejected_claims_path)

print(f"[SUCCESS] gold_rejected_claims written to: {gold_rejected_claims_path}")

# COMMAND ----------

# =========================================================
# GOLD TABLE 8: Patient Status Summary
# Business Q: How many patients are currently admitted vs discharged?
# =========================================================

gold_patient_status_df = (
    fact_admission_df
    .withColumn(
        "patient_status",
        when(col("is_currently_admitted") == True, "Currently Admitted")
        .otherwise("Discharged")
    )
    .groupBy("patient_status")
    .agg(
        count("admission_id").alias("patient_count")
    )
    .withColumn("report_month", lit(prev_month_full_name))
)

display(gold_patient_status_df)

# COMMAND ----------

# Write Gold Table 7 (Patient Status) to ADLS as Parquet
gold_patient_status_df \
    .coalesce(1) \
    .write \
    .format("parquet") \
    .mode("overwrite") \
    .save(gold_patient_status_path)

print(f"[SUCCESS] gold_patient_status written to: {gold_patient_status_path}")

# COMMAND ----------

# =========================================================
# AUDIT LOGGING
# =========================================================

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType
)

audit_schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("notebook_name", StringType(), True),
    StructField("run_id",        StringType(), True),
    StructField("source",        StringType(), True),
    StructField("layer",         StringType(), True),
    StructField("record_count",  IntegerType(), True),
    StructField("load_time",     DateType(),   True),
    StructField("status",        StringType(), True),
    StructField("report_month",  StringType(), True),
])

def write_audit_record(df, gold_table_name, report_month):
    audit_df = spark.createDataFrame(
        [(pipeline_name, notebook_name, run_id, gold_table_name,
          "gold", df.count(), date.today(), "Success", report_month)],
        schema=audit_schema
    )
    audit_df.write.mode("append").saveAsTable("J2D_Audit_table")
    print(f"  Audit record written for {gold_table_name}")

print("Writing audit records for all Gold tables...")
write_audit_record(gold_medicine_availability_df, "gold_medicine_availability", prev_month_full_name)
write_audit_record(gold_device_availability_df,   "gold_device_availability",   prev_month_full_name)
write_audit_record(gold_finance_weekly_df,         "gold_finance_weekly",         prev_month_full_name)
write_audit_record(gold_weekly_admissions_df,      "gold_weekly_admissions",      prev_month_full_name)
write_audit_record(gold_claim_status_summary_df,   "gold_claim_status_summary",   prev_month_full_name)
write_audit_record(gold_rejected_claims_df,        "gold_rejected_claims",        prev_month_full_name)
write_audit_record(gold_patient_status_df,         "gold_patient_status",         prev_month_full_name)
print("All audit records written successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM J2D_Audit_table WHERE layer = 'gold' ORDER BY load_time DESC

# COMMAND ----------

# -------------------------
# NOTEBOOK EXIT
# -------------------------
success = "Gold layer processing completed successfully. All 7 Gold tables written to ADLS goldlayer as Parquet."
print(success)
dbutils.notebook.exit(success)