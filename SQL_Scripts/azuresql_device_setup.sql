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

-- STEP 3: Load data – complete INSERT for all 50 rows (10 hospitals x 5 devices)
-- Source: Device_events_January_2026.csv
INSERT INTO dbo.device_events (hospital_id, device_id, device_type, total_units, in_use_units, week) VALUES
('H01','DEV001','Ventilator',35,18,'2025-M01'),
('H01','DEV002','Patient Monitor',124,115,'2025-M01'),
('H01','DEV003','Infusion Pump',158,149,'2025-M01'),
('H01','DEV004','Dialysis Machine',32,20,'2025-M01'),
('H01','DEV005','ECG Machine',46,40,'2025-M01'),
('H02','DEV001','Ventilator',47,46,'2025-M01'),
('H02','DEV002','Patient Monitor',132,123,'2025-M01'),
('H02','DEV003','Infusion Pump',102,92,'2025-M01'),
('H02','DEV004','Dialysis Machine',32,28,'2025-M01'),
('H02','DEV005','ECG Machine',38,32,'2025-M01'),
('H03','DEV001','Ventilator',45,34,'2025-M01'),
('H03','DEV002','Patient Monitor',94,50,'2025-M01'),
('H03','DEV003','Infusion Pump',100,98,'2025-M01'),
('H03','DEV004','Dialysis Machine',29,15,'2025-M01'),
('H03','DEV005','ECG Machine',24,19,'2025-M01'),
('H04','DEV001','Ventilator',35,23,'2025-M01'),
('H04','DEV002','Patient Monitor',110,85,'2025-M01'),
('H04','DEV003','Infusion Pump',169,107,'2025-M01'),
('H04','DEV004','Dialysis Machine',35,20,'2025-M01'),
('H04','DEV005','ECG Machine',25,21,'2025-M01'),
('H05','DEV001','Ventilator',37,20,'2025-M01'),
('H05','DEV002','Patient Monitor',112,89,'2025-M01'),
('H05','DEV003','Infusion Pump',108,102,'2025-M01'),
('H05','DEV004','Dialysis Machine',30,29,'2025-M01'),
('H05','DEV005','ECG Machine',34,21,'2025-M01'),
('H06','DEV001','Ventilator',32,24,'2025-M01'),
('H06','DEV002','Patient Monitor',86,71,'2025-M01'),
('H06','DEV003','Infusion Pump',180,147,'2025-M01'),
('H06','DEV004','Dialysis Machine',29,16,'2025-M01'),
('H06','DEV005','ECG Machine',24,23,'2025-M01'),
('H07','DEV001','Ventilator',36,34,'2025-M01'),
('H07','DEV002','Patient Monitor',129,113,'2025-M01'),
('H07','DEV003','Infusion Pump',139,117,'2025-M01'),
('H07','DEV004','Dialysis Machine',22,16,'2025-M01'),
('H07','DEV005','ECG Machine',42,23,'2025-M01'),
('H08','DEV001','Ventilator',39,21,'2025-M01'),
('H08','DEV002','Patient Monitor',80,50,'2025-M01'),
('H08','DEV003','Infusion Pump',111,91,'2025-M01'),
('H08','DEV004','Dialysis Machine',24,13,'2025-M01'),
('H08','DEV005','ECG Machine',41,36,'2025-M01'),
('H09','DEV001','Ventilator',32,28,'2025-M01'),
('H09','DEV002','Patient Monitor',133,73,'2025-M01'),
('H09','DEV003','Infusion Pump',126,65,'2025-M01'),
('H09','DEV004','Dialysis Machine',18,15,'2025-M01'),
('H09','DEV005','ECG Machine',43,22,'2025-M01'),
('H10','DEV001','Ventilator',49,34,'2025-M01'),
('H10','DEV002','Patient Monitor',118,115,'2025-M01'),
('H10','DEV003','Infusion Pump',175,157,'2025-M01'),
('H10','DEV004','Dialysis Machine',39,20,'2025-M01'),
('H10','DEV005','ECG Machine',46,31,'2025-M01');

-- STEP 4: Create ADF read user (least privilege)
-- CREATE LOGIN adf_device_reader WITH PASSWORD = 'J2D@Adf2026!';
-- CREATE USER adf_device_reader FOR LOGIN adf_device_reader;
-- GRANT SELECT ON dbo.device_events TO adf_device_reader;

-- STEP 5: Verify
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
