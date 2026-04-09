%pip install azure-identity azure-keyvault-secrets
dbutils.library.restartPython()

# Databricks notebook source
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import col, split, regexp_replace


# COMMAND ----------

from pyspark.sql.functions import length

def check_string_length(df, colname, max_length):
    if df.filter(length(colname) > max_length).count() > 0:
        dbutils.notebook.exit(f"String length check failed: Values in column '{colname}' exceed {max_length} characters.")

# COMMAND ----------

def check_column_is_int(df, colname):
    int_types = ['int', 'int32', 'int64', 'bigint', 'smallint', 'tinyint']
    if dict(df.dtypes)[colname] not in int_types:
        nulls_before = df.filter(col(colname).isNull()).count()
        casted_df = df.withColumn(colname, col(colname).cast('int'))
        nulls_after = casted_df.filter(col(colname).isNull()).count()
        if nulls_after > nulls_before:
            dbutils.notebook.exit(f"Column '{colname}' could not be cast to integer without data loss.")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, current_date

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



