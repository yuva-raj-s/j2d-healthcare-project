-- ============================================================
-- J2D Healthcare Project
-- Azure Synapse Analytics – Serverless SQL Pool (j2d-synapse-101)
-- External Tables pointing to Gold Parquet files in ADLS Gen2 (j2dstorage101)
-- Run in: Synapse Studio → Develop → New SQL Script
-- Pool   : Built-in (Serverless SQL Pool – FREE per query)
-- ============================================================

-- 1. Drop the external tables first
DROP EXTERNAL TABLE gold.medicine_availability;
DROP EXTERNAL TABLE gold.device_availability;
DROP EXTERNAL TABLE gold.finance_weekly;
DROP EXTERNAL TABLE gold.weekly_admissions;
DROP EXTERNAL TABLE gold.claim_status_summary;
DROP EXTERNAL TABLE gold.rejected_claims;
DROP EXTERNAL TABLE gold.patient_status;
GO

-- 2. Drop the data source
DROP EXTERNAL DATA SOURCE GoldLayerDataSource;
GO

-- 3. Now drop and recreate the credential
DROP DATABASE SCOPED CREDENTIAL SynapseGoldCredential;
GO

CREATE DATABASE SCOPED CREDENTIAL SynapseGoldCredential
WITH IDENTITY = 'Managed Identity';
GO

-- 4. Recreate the data source
CREATE EXTERNAL DATA SOURCE GoldLayerDataSource
WITH (
    LOCATION   = 'abfss://goldlayer@j2dstorage101.dfs.core.windows.net',
    CREDENTIAL = SynapseGoldCredential
);
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

