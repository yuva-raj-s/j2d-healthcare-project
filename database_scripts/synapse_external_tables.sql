-- ============================================================
-- J2D Healthcare Project
-- Azure Synapse Analytics - Serverless SQL Pool (j2d-synapse-101)
-- External Tables pointing to Gold Parquet files in ADLS Gen2 (j2dstorage101)
-- Run in: Synapse Studio → Develop → New SQL Script
-- Pool   : Built-in (Serverless SQL Pool - FREE per query)
-- ============================================================

-- ============================================================
-- STEP 1: Master Key
-- Required before any DATABASE SCOPED CREDENTIAL can be created.
-- Safe to run multiple times — IF NOT EXISTS guard prevents errors.
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'J2D@Healthcare#2026!';
END
GO

-- ============================================================
-- STEP 2: Drop external tables safely (only if they exist)
-- ============================================================
IF OBJECT_ID('gold.medicine_availability',  'U') IS NOT NULL DROP EXTERNAL TABLE gold.medicine_availability;
IF OBJECT_ID('gold.device_availability',    'U') IS NOT NULL DROP EXTERNAL TABLE gold.device_availability;
IF OBJECT_ID('gold.finance_weekly',         'U') IS NOT NULL DROP EXTERNAL TABLE gold.finance_weekly;
IF OBJECT_ID('gold.weekly_admissions',      'U') IS NOT NULL DROP EXTERNAL TABLE gold.weekly_admissions;
IF OBJECT_ID('gold.claim_status_summary',   'U') IS NOT NULL DROP EXTERNAL TABLE gold.claim_status_summary;
IF OBJECT_ID('gold.rejected_claims',        'U') IS NOT NULL DROP EXTERNAL TABLE gold.rejected_claims;
IF OBJECT_ID('gold.patient_status',         'U') IS NOT NULL DROP EXTERNAL TABLE gold.patient_status;
GO

-- ============================================================
-- STEP 3: Drop data source and credential safely
-- ============================================================
IF EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'GoldLayerDataSource')
    DROP EXTERNAL DATA SOURCE GoldLayerDataSource;
GO

IF EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'SynapseGoldCredential')
    DROP DATABASE SCOPED CREDENTIAL SynapseGoldCredential;
GO

-- ============================================================
-- STEP 4: Recreate credential using Synapse Managed Identity
-- ============================================================
CREATE DATABASE SCOPED CREDENTIAL SynapseGoldCredential
WITH IDENTITY = 'Managed Identity';
GO

-- ============================================================
-- STEP 5: Recreate external data source
-- Points to the goldlayer container in j2dstorage101
-- ============================================================
CREATE EXTERNAL DATA SOURCE GoldLayerDataSource
WITH (
    LOCATION   = 'abfss://goldlayer@j2dstorage02.dfs.core.windows.net',
    CREDENTIAL = SynapseGoldCredential
);
GO

-- ============================================================
-- STEP 6: Create Parquet file format (safe — only if missing)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'ParquetFormat')
    CREATE EXTERNAL FILE FORMAT ParquetFormat
    WITH (FORMAT_TYPE = PARQUET);
GO

-- ============================================================
-- STEP 7: Create gold schema (safe — only if missing)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC ('CREATE SCHEMA gold');
GO

-- ============================================================
-- EXTERNAL TABLE 1: Medicine Availability
-- ============================================================
CREATE EXTERNAL TABLE gold.medicine_availability (
    hospital_id     NVARCHAR(20),
    medicine_name   NVARCHAR(100),
    category        NVARCHAR(50),
    available_stock INT,
    expiry_date     DATE,
    report_month    NVARCHAR(20)
)
WITH (
    DATA_SOURCE = GoldLayerDataSource,
    LOCATION    = '/Gold_Data/gold_medicine_availability/',
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================
-- EXTERNAL TABLE 2: Device Availability
-- ============================================================
CREATE EXTERNAL TABLE gold.device_availability (
    hospital_id     NVARCHAR(20),
    device_type     NVARCHAR(80),
    total_units     BIGINT,
    in_use_units    BIGINT,
    available_units BIGINT,
    utilization_pct FLOAT,
    report_month    NVARCHAR(20)
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
CREATE EXTERNAL TABLE gold.finance_weekly (
    week_start_date        DATE,
    insurance_id           NVARCHAR(20),
    insurance_name         NVARCHAR(100),
    total_claims           BIGINT,
    total_billed_amount    FLOAT,
    total_approved_amount  FLOAT,
    revenue_leakage_amount FLOAT,
    approval_percentage    FLOAT,
    report_month           NVARCHAR(20)
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
CREATE EXTERNAL TABLE gold.claim_status_summary (
    insurance_name NVARCHAR(100),
    claim_status   NVARCHAR(20),
    claim_count    BIGINT,
    report_month   NVARCHAR(20)
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
CREATE EXTERNAL TABLE gold.rejected_claims (
    insurance_name       NVARCHAR(100),
    rejected_claim_count BIGINT,
    report_month         NVARCHAR(20)
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
CREATE EXTERNAL TABLE gold.patient_status (
    patient_status NVARCHAR(50),
    patient_count  BIGINT,
    report_month   NVARCHAR(20)
)
WITH (
    DATA_SOURCE = GoldLayerDataSource,
    LOCATION    = '/Gold_Data/gold_patient_status/',
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================
-- STEP 8: Verify all external tables
-- ============================================================
SELECT COUNT(*) AS medicine_rows  FROM gold.medicine_availability;
SELECT COUNT(*) AS device_rows    FROM gold.device_availability;
SELECT COUNT(*) AS finance_rows   FROM gold.finance_weekly;
SELECT COUNT(*) AS admission_rows FROM gold.weekly_admissions;
SELECT COUNT(*) AS claim_rows     FROM gold.claim_status_summary;
SELECT COUNT(*) AS rejected_rows  FROM gold.rejected_claims;
SELECT COUNT(*) AS patient_rows   FROM gold.patient_status;
