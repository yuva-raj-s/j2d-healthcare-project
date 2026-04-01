-- ============================================================
-- J2D Healthcare Project
-- Azure SQL Database Setup Script – Device Source
-- Server : j2d-sqlserver.database.windows.net
-- Database: j2d_device_db
-- Table   : device_events
-- Run this in Azure Query Editor or SSMS
-- ============================================================

-- STEP 1: Create the device_events table
-- (Database j2d_device_db is created in Azure Portal - no CREATE DATABASE in Azure SQL)

DROP TABLE IF EXISTS dbo.device_events;

CREATE TABLE dbo.device_events (
    hospital_id     NVARCHAR(20)    NOT NULL,
    device_id       NVARCHAR(20)    NOT NULL,
    device_type     NVARCHAR(80),
    total_units     INT,
    in_use_units    INT,
    week            NVARCHAR(20),
    CONSTRAINT PK_device_events PRIMARY KEY (hospital_id, device_id, week)
);

-- STEP 2: Create index for ADF reads
CREATE NONCLUSTERED INDEX IX_device_events_hospital 
    ON dbo.device_events (hospital_id);

-- STEP 3: Load data
-- Option A: Use ADF Copy Activity (recommended, source = Blob CSV → sink = Azure SQL table)
-- Option B: BCP utility from local machine
-- bcp j2d_device_db.dbo.device_events IN Device_events_January_2026.csv -c -t, -S j2d-sqlserver.database.windows.net -U adminuser -P YourPassword! -F 2

-- STEP 4: Create ADF read user (least privilege)
-- CREATE LOGIN adf_device_reader WITH PASSWORD = 'J2D@Adf2026!';
-- CREATE USER adf_device_reader FOR LOGIN adf_device_reader;
-- GRANT SELECT ON dbo.device_events TO adf_device_reader;

-- STEP 5: Reference INSERT – first 5 rows (for manual testing)
INSERT INTO dbo.device_events VALUES
('H01','DEV001','Ventilator',35,18,'2025-M01'),
('H01','DEV002','Patient Monitor',124,115,'2025-M01'),
('H01','DEV003','Infusion Pump',158,149,'2025-M01'),
('H01','DEV004','Dialysis Machine',32,20,'2025-M01'),
('H01','DEV005','ECG Machine',46,40,'2025-M01');

-- STEP 6: Verify
SELECT COUNT(*) AS total_records FROM dbo.device_events;
SELECT TOP 5 * FROM dbo.device_events;

-- ============================================================
-- Azure SQL Free Tier Settings (use in Portal when creating):
-- Service Tier      : General Purpose – Serverless
-- vCores            : 1 (min) / 1 (max)
-- Data max size     : 32 GB
-- Auto-pause delay  : 60 minutes (saves credit)
-- Backup redundancy : Locally redundant (cheapest)
-- ============================================================
