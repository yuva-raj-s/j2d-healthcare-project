# Databricks notebook source
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import input_file_name, col, split, regexp_replace, length, to_timestamp, current_date, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# -------------------------------------------------------------------------
# 1. Validation Helper Functions (Retained from original Helper_NB)
# -------------------------------------------------------------------------

def check_string_length(df, colname, max_length):
    if df.filter(length(colname) > max_length).count() > 0:
        dbutils.notebook.exit(f"String length check failed: Values in column '{colname}' exceed {max_length} characters.")

def check_column_is_int(df, colname):
    int_types = ['int', 'int32', 'int64', 'bigint', 'smallint', 'tinyint']
    if dict(df.dtypes)[colname] not in int_types:
        nulls_before = df.filter(col(colname).isNull()).count()
        casted_df = df.withColumn(colname, col(colname).cast('int'))
        nulls_after = casted_df.filter(col(colname).isNull()).count()
        if nulls_after > nulls_before:
            dbutils.notebook.exit(f"Column '{colname}' could not be cast to integer without data loss.")

def validate_gender(df, gender_col="patient_gender", valid_genders=("M", "F", "O")):
    invalid = df.filter(~col(gender_col).isin(*valid_genders))
    if invalid.count() > 0:
        dbutils.notebook.exit("Invalid gender values found in data")
    return df

def validate_dob(df, dob_col="patient_dob"):
    invalid = df.filter(col(dob_col) > current_date())
    if invalid.count() > 0:
        dbutils.notebook.exit("Invalid DOB values found in data")
    return df

def validate_admission_discharge(df, admission_col="admission_time", discharge_col="discharge_time"):
    invalid = df.filter(
        col(discharge_col).isNotNull() & (col(admission_col) > col(discharge_col))
    )
    if invalid.count() > 0:
        dbutils.notebook.exit("Admission time after discharge time found in data")
    return df

def validate_col_greater_equal(df, col1, col2):
    invalid = df.filter(col(col1) < col(col2))
    if invalid.count() > 0:
        dbutils.notebook.exit(f"{col1} is less than {col2} in data")
    return df


# -------------------------------------------------------------------------
# 2. Audit Table Schema & Standardized Writing Function
# -------------------------------------------------------------------------
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

def write_audit_record(pipeline_name, notebook_name, run_id, source, layer, count, status, report_month):
    audit_df = spark.createDataFrame(
        [(pipeline_name, notebook_name, run_id, source, layer, count, date.today(), status, report_month)],
        schema=audit_schema
    )
    # Using mergeSchema incase a new column like error_message gets added downstream
    audit_df.write.mode("append").option("mergeSchema", "true").saveAsTable("J2D_Audit_table")


# -------------------------------------------------------------------------
# 3. Dedicated Notebook Activity Execution (for metadata init & fail hooks)
# -------------------------------------------------------------------------
try:
    dbutils.widgets.text("audit_action", "") 
    dbutils.widgets.text("pipeline_name", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("report_month", "")
    dbutils.widgets.text("error_message", "")
    
    audit_action_wg = dbutils.widgets.get("audit_action")
    pipeline_name_wg = dbutils.widgets.get("pipeline_name")
    run_id_wg = dbutils.widgets.get("run_id")
    report_month_wg = dbutils.widgets.get("report_month")
    error_message_wg = dbutils.widgets.get("error_message")

    if audit_action_wg == "START":
        write_audit_record(pipeline_name_wg, "Pipeline_Start", run_id_wg, "ALL", "Init", 0, "RUNNING", report_month_wg)
        dbutils.notebook.exit("AUDIT_START_SUCCESS")
        
    elif audit_action_wg == "FAIL_PIPELINE":
        status_msg = f"FAILED: {error_message_wg}"
        write_audit_record(pipeline_name_wg, "Pipeline_Fail", run_id_wg, "ALL", "Any", 0, status_msg, report_month_wg)
        dbutils.notebook.exit("AUDIT_FAIL_RECORDED")
        
except Exception as e:
    # Safely bypass if widgets aren't set (e.g. when %run is called from another notebook)
    pass
