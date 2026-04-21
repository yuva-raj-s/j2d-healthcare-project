# Databricks notebook source
# =========================================================================
# Helper_NB  —  Shared bootstrap for J2D Healthcare Pipeline
#
# %run this notebook at the top of every pipeline notebook:
#   # MAGIC %run "/Workspace/Users/<your-email>/Helper_NB"
#
# Provides after %run:
#   - All common imports
#   - Key Vault secrets (mysql_password, azuresql_password,
#                        postgresql_password, adls_account_key, synapse_password)
#   - ADLS Spark conf already set  (storage_account_name)
#   - CATALOG, AUDIT_TABLE constants
#   - Validation helper functions
#   - audit_schema + write_audit_record()
#   - Optional audit-action dispatch (START / FAIL_PIPELINE from ADF)
# =========================================================================

# COMMAND ----------

# -------------------------------------------------------------------------
# 1. Install Azure SDK libraries
#    NOTE: In Databricks Runtime 11+, %pip install automatically restarts
#    the Python interpreter — do NOT call dbutils.library.restartPython()
#    after this cell, as it would re-restart the kernel unnecessarily.
# -------------------------------------------------------------------------
# MAGIC %pip install azure-identity azure-keyvault-secrets --quiet

# COMMAND ----------

# -------------------------------------------------------------------------
# 2. Common imports
# -------------------------------------------------------------------------
import os
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, split, regexp_replace, length,
    to_timestamp, to_date, current_date, current_timestamp,
    input_file_name, when, datediff
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType
)

# COMMAND ----------

# -------------------------------------------------------------------------
# 3. Key Vault — retrieve all secrets once
# -------------------------------------------------------------------------
KV_URI = "https://j2d-keyvault101.vault.azure.net/"
_kv_credential = DefaultAzureCredential()
_kv_client      = SecretClient(vault_url=KV_URI, credential=_kv_credential)

mysql_password      = _kv_client.get_secret("mysql-password").value
azuresql_password   = _kv_client.get_secret("azuresql-password").value
postgresql_password = _kv_client.get_secret("postgresql-password").value
adls_account_key    = _kv_client.get_secret("adls-account-key").value
synapse_password    = _kv_client.get_secret("synapse-password").value

print("Key Vault secrets loaded.")

# COMMAND ----------

# -------------------------------------------------------------------------
# 4. Storage & Unity Catalog configuration
# -------------------------------------------------------------------------
storage_account_name = "j2dstorage101"

# Configure ADLS Gen2 access for the entire Spark session
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    adls_account_key
)

# Unity Catalog — all managed objects live under this catalog
CATALOG     = "j2d_databricks_04"
AUDIT_TABLE = f"{CATALOG}.default.J2D_Audit_table"

print(f"Storage configured for: {storage_account_name}")
print(f"Unity Catalog: {CATALOG}  |  Audit table: {AUDIT_TABLE}")

# COMMAND ----------

# -------------------------------------------------------------------------
# 5. Validation helper functions
# -------------------------------------------------------------------------

def check_string_length(df, colname, max_length):
    """Exit notebook if any value in colname exceeds max_length characters."""
    if df.filter(length(col(colname)) > max_length).count() > 0:
        dbutils.notebook.exit(
            f"String length check failed: '{colname}' has values > {max_length} chars."
        )


def check_column_is_int(df, colname):
    """Exit notebook if colname cannot be safely cast to integer."""
    int_types = ["int", "int32", "int64", "bigint", "smallint", "tinyint"]
    if dict(df.dtypes).get(colname) not in int_types:
        nulls_before = df.filter(col(colname).isNull()).count()
        nulls_after  = df.withColumn(colname, col(colname).cast("int")) \
                         .filter(col(colname).isNull()).count()
        if nulls_after > nulls_before:
            dbutils.notebook.exit(
                f"Column '{colname}' cannot be cast to int without data loss."
            )


def validate_gender(df, gender_col="patient_gender", valid_genders=("M", "F", "O")):
    """Exit notebook if gender column contains values outside the allowed set."""
    if df.filter(~col(gender_col).isin(*valid_genders)).count() > 0:
        dbutils.notebook.exit("Invalid gender values found in data.")
    return df


def validate_dob(df, dob_col="patient_dob"):
    """Exit notebook if any DOB is in the future."""
    if df.filter(col(dob_col) > current_date()).count() > 0:
        dbutils.notebook.exit("Invalid DOB values found — future dates detected.")
    return df


def validate_admission_discharge(df,
                                  admission_col="admission_time",
                                  discharge_col="discharge_time"):
    """Exit notebook if admission time is after discharge time."""
    invalid = df.filter(
        col(discharge_col).isNotNull() & (col(admission_col) > col(discharge_col))
    )
    if invalid.count() > 0:
        dbutils.notebook.exit("Admission time is after discharge time in data.")
    return df


def validate_col_greater_equal(df, col1, col2):
    """Exit notebook if col1 < col2 for any row."""
    if df.filter(col(col1) < col(col2)).count() > 0:
        dbutils.notebook.exit(f"{col1} is less than {col2} in data.")
    return df

print("Validation helpers loaded.")

# COMMAND ----------

# -------------------------------------------------------------------------
# 6. Audit schema & write function
#    write_audit_record() is the ONLY place audit rows are created.
#    Signature: (pipeline_name, notebook_name, run_id, source,
#                layer, count, status, report_month)
# -------------------------------------------------------------------------
audit_schema = StructType([
    StructField("pipeline_name", StringType(),  True),
    StructField("notebook_name", StringType(),  True),
    StructField("run_id",        StringType(),  True),
    StructField("source",        StringType(),  True),
    StructField("layer",         StringType(),  True),
    StructField("record_count",  IntegerType(), True),
    StructField("load_time",     DateType(),    True),
    StructField("status",        StringType(),  True),
    StructField("report_month",  StringType(),  True),
])


def write_audit_record(pipeline_name, notebook_name, run_id, source,
                       layer, count, status, report_month):
    """Append a single audit row to the Unity Catalog audit table."""
    audit_df = spark.createDataFrame(
        [(pipeline_name, notebook_name, run_id, source,
          layer, int(count), date.today(), status, report_month)],
        schema=audit_schema
    )
    audit_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(AUDIT_TABLE)


print("Audit helpers loaded.")

# COMMAND ----------

# -------------------------------------------------------------------------
# 7. Optional: ADF audit-action dispatcher
#    When ADF calls Helper_NB directly (e.g. as a Notebook Activity with
#    audit_action=START or FAIL_PIPELINE), this block handles the request
#    and exits immediately.  When %run is used from another notebook the
#    widgets will be empty and this block is silently skipped.
# -------------------------------------------------------------------------
try:
    dbutils.widgets.text("audit_action",   "")
    dbutils.widgets.text("pipeline_name",  "")
    dbutils.widgets.text("run_id",         "")
    dbutils.widgets.text("report_month",   "")
    dbutils.widgets.text("error_message",  "")

    _audit_action   = dbutils.widgets.get("audit_action")
    _pipeline_name  = dbutils.widgets.get("pipeline_name")
    _run_id         = dbutils.widgets.get("run_id")
    _report_month   = dbutils.widgets.get("report_month")
    _error_message  = dbutils.widgets.get("error_message")

    if _audit_action == "START":
        write_audit_record(
            _pipeline_name, "Pipeline_Start", _run_id,
            "ALL", "Init", 0, "RUNNING", _report_month
        )
        dbutils.notebook.exit("AUDIT_START_SUCCESS")

    elif _audit_action == "FAIL_PIPELINE":
        write_audit_record(
            _pipeline_name, "Pipeline_Fail", _run_id,
            "ALL", "Any", 0, f"FAILED: {_error_message}", _report_month
        )
        dbutils.notebook.exit("AUDIT_FAIL_RECORDED")

except Exception:
    # Silently skip when %run is called from another notebook
    pass
