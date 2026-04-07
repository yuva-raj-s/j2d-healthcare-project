-- ============================================================
-- J2D Healthcare Project
-- Azure Synapse Analytics – Serverless SQL Pool (j2d-synapse-101)
-- External Tables pointing to Gold Parquet files in ADLS Gen2 (j2dstorage101)
-- Run in: Synapse Studio → Develop → New SQL Script
-- Pool   : Built-in (Serverless SQL Pool – FREE per query)
-- ============================================================

-- ============================================================
-- STEP 1: Create a dedicated database in Serverless pool
-- ============================================================
CREATE DATABASE IF NOT EXISTS j2d_gold_db;
GO

USE j2d_gold_db;
GO

-- ============================================================
-- STEP 2: Create Master Key (required for credential)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'J2D@Synapse#2026!';
END
GO

-- ============================================================
-- STEP 3: Create Database Scoped Credential
-- Uses Managed Identity of Synapse workspace (no key needed)
-- Make sure your Synapse Managed Identity (j2d-synapse-101) has "Storage Blob Data Reader"
-- role on the j2dstorage101 ADLS account.
-- ============================================================
CREATE DATABASE SCOPED CREDENTIAL SynapseGoldCredential
WITH IDENTITY = 'Managed Identity';
GO

-- ============================================================
-- STEP 4: Create External Data Source – pointing to goldlayer
-- Replace j2dstorage101 with your actual storage account name
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'GoldLayerDataSource')
BEGIN
    CREATE EXTERNAL DATA SOURCE GoldLayerDataSource
    WITH (
        LOCATION   = 'abfss://goldlayer@j2dstorage101.dfs.core.windows.net',
        CREDENTIAL = SynapseGoldCredential
    );
END
GO

-- ============================================================
-- STEP 5: Create External File Format – Parquet
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'ParquetFormat')
BEGIN
    CREATE EXTERNAL FILE FORMAT ParquetFormat
    WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );
END
GO

-- ============================================================
-- STEP 6: Create Gold Schema
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC ('CREATE SCHEMA gold');
GO

-- ============================================================
-- EXTERNAL TABLE 1: Medicine Availability
-- ============================================================
IF OBJECT_ID('gold.medicine_availability', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.medicine_availability;

CREATE EXTERNAL TABLE gold.medicine_availability (
    hospital_id     NVARCHAR(20),
    medicine_name   NVARCHAR(100),
    category        NVARCHAR(50),
    available_stock INT,
    expiry_date     DATE,
    report_month    NVARCHAR(20)
)
WITH (
    DATA_SOURCE       = GoldLayerDataSource,
    LOCATION          = '/Gold_Data/gold_medicine_availability/',
    FILE_FORMAT       = ParquetFormat
);
GO

-- ============================================================
-- EXTERNAL TABLE 2: Device Availability
-- ============================================================
IF OBJECT_ID('gold.device_availability', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.device_availability;

CREATE EXTERNAL TABLE gold.device_availability (
    hospital_id      NVARCHAR(20),
    device_type      NVARCHAR(80),
    total_units      BIGINT,
    in_use_units     BIGINT,
    available_units  BIGINT,
    utilization_pct  FLOAT,
    report_month     NVARCHAR(20)
)
WITH (
    DATA_SOURCE = GoldLayerDataSource,
    LOCATION    = '/Gold_Data/gold_device_availability/',
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================
-- EXTERNAL TABLE 3: Finance Weekly Summary
-- ============================================================
IF OBJECT_ID('gold.finance_weekly', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.finance_weekly;

CREATE EXTERNAL TABLE gold.finance_weekly (
    week_start_date         DATE,
    insurance_id            NVARCHAR(20),
    insurance_name          NVARCHAR(100),
    total_claims            BIGINT,
    total_billed_amount     FLOAT,
    total_approved_amount   FLOAT,
    revenue_leakage_amount  FLOAT,
    approval_percentage     FLOAT,
    report_month            NVARCHAR(20)
)
WITH (
    DATA_SOURCE = GoldLayerDataSource,
    LOCATION    = '/Gold_Data/gold_finance_weekly/',
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================
-- EXTERNAL TABLE 4: Weekly Admissions Trend
-- ============================================================
IF OBJECT_ID('gold.weekly_admissions', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.weekly_admissions;

CREATE EXTERNAL TABLE gold.weekly_admissions (
    week_start_date  DATE,
    total_admissions BIGINT,
    report_month     NVARCHAR(20)
)
WITH (
    DATA_SOURCE = GoldLayerDataSource,
    LOCATION    = '/Gold_Data/gold_weekly_admissions/',
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================
-- EXTERNAL TABLE 5: Claim Status Summary
-- ============================================================
IF OBJECT_ID('gold.claim_status_summary', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.claim_status_summary;

CREATE EXTERNAL TABLE gold.claim_status_summary (
    insurance_name  NVARCHAR(100),
    claim_status    NVARCHAR(20),
    claim_count     BIGINT,
    report_month    NVARCHAR(20)
)
WITH (
    DATA_SOURCE = GoldLayerDataSource,
    LOCATION    = '/Gold_Data/gold_claim_status_summary/',
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================
-- EXTERNAL TABLE 6: Rejected Claims Ranking
-- ============================================================
IF OBJECT_ID('gold.rejected_claims', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.rejected_claims;

CREATE EXTERNAL TABLE gold.rejected_claims (
    insurance_name        NVARCHAR(100),
    rejected_claim_count  BIGINT,
    report_month          NVARCHAR(20)
)
WITH (
    DATA_SOURCE = GoldLayerDataSource,
    LOCATION    = '/Gold_Data/gold_rejected_claims/',
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================
-- EXTERNAL TABLE 7: Patient Status Summary
-- ============================================================
IF OBJECT_ID('gold.patient_status', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.patient_status;

CREATE EXTERNAL TABLE gold.patient_status (
    patient_status  NVARCHAR(50),
    patient_count   BIGINT,
    report_month    NVARCHAR(20)
)
WITH (
    DATA_SOURCE = GoldLayerDataSource,
    LOCATION    = '/Gold_Data/gold_patient_status/',
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================
-- VERIFY ALL EXTERNAL TABLES
-- ============================================================
SELECT COUNT(*) AS medicine_rows     FROM gold.medicine_availability;
SELECT COUNT(*) AS device_rows       FROM gold.device_availability;
SELECT COUNT(*) AS finance_rows      FROM gold.finance_weekly;
SELECT COUNT(*) AS admission_rows    FROM gold.weekly_admissions;
SELECT COUNT(*) AS claim_rows        FROM gold.claim_status_summary;
SELECT COUNT(*) AS rejected_rows     FROM gold.rejected_claims;
SELECT COUNT(*) AS patient_rows      FROM gold.patient_status;

