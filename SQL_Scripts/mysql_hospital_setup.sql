-- ============================================================
-- J2D Healthcare Project
-- MySQL Setup Script – Hospital Source (Azure MySQL Flexible Server)
-- Target DB : j2d_hospital_db
-- Table     : hospital_events
-- Run this in MySQL Workbench or Azure Cloud Shell (MySQL CLI)
-- ============================================================

-- STEP 1: Create the database
CREATE DATABASE IF NOT EXISTS j2d_hospital_db 
    CHARACTER SET utf8mb4 
    COLLATE utf8mb4_unicode_ci;

USE j2d_hospital_db;

-- STEP 2: Create the hospital_events table
DROP TABLE IF EXISTS hospital_events;

CREATE TABLE hospital_events (
    event_id        VARCHAR(20)     NOT NULL,
    patient_id      VARCHAR(20)     NOT NULL,
    patient_gender  VARCHAR(5),
    patient_dob     DATE,
    insurance_id    VARCHAR(20),
    insurance_name  VARCHAR(100),
    department      VARCHAR(80),
    doctor_id       VARCHAR(20),
    bed_id          VARCHAR(20),
    bed_type        VARCHAR(30),
    hospital_id     VARCHAR(20),
    admission_time  DATETIME,
    discharge_time  DATETIME,
    billed_amount   INT,
    approved_amount INT,
    claim_status    VARCHAR(20),
    PRIMARY KEY (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- STEP 3: Load data (run LOAD DATA from CSV)
-- Option A – LOAD DATA LOCAL INFILE (if MySQL allows it)
-- LOAD DATA LOCAL INFILE '/path/to/Hospital_events_January_2026.csv'
-- INTO TABLE hospital_events
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS
-- (event_id, patient_id, patient_gender, @patient_dob, insurance_id, insurance_name,
--  department, doctor_id, bed_id, bed_type, hospital_id,
--  @admission_time, @discharge_time, billed_amount, approved_amount, claim_status)
-- SET patient_dob    = NULLIF(STR_TO_DATE(@patient_dob, '%Y-%m-%d'), ''),
--     admission_time = NULLIF(STR_TO_DATE(@admission_time, '%Y-%m-%d %H:%i:%s'), ''),
--     discharge_time = NULLIF(STR_TO_DATE(@discharge_time, '%Y-%m-%d %H:%i:%s'), '');

-- Option B – Use Azure Data Factory to copy directly (recommended for this project)
-- ADF Copy Activity will copy from Azure Blob (staging) to MySQL table via Linked Service.

-- STEP 4: Create a dedicated ADF user (least-privilege)
-- CREATE USER 'adf_user'@'%' IDENTIFIED BY 'J2D@Adf2026!';
-- GRANT SELECT, INSERT, UPDATE ON j2d_hospital_db.* TO 'adf_user'@'%';
-- FLUSH PRIVILEGES;

-- STEP 5: Verify
SELECT COUNT(*) AS total_records FROM hospital_events;
SELECT * FROM hospital_events LIMIT 5;

-- ============================================================
-- REFERENCE: Sample INSERT for manual testing (first 3 rows)
-- ============================================================
INSERT INTO hospital_events VALUES
('E1','P1','M','1985-05-10','INS1','Star Health Insurance','Cardiology','D1','B1','ICU','H1','2025-11-20 10:00:00','2025-11-25 09:00:00',80000,70000,'APPROVED'),
('E2','P2','F','1990-03-15','INS2','HDFC ERGO Health Insurance','Orthopedics','D2','B2','General','H1','2025-11-22 14:00:00',NULL,45000,45000,'APPROVED'),
('E3','P3','X','2030-01-01','INS3','ICICI Lombard Health Insurance','Neurology','D3','B3','ICU','H2','2025-11-23 11:00:00','2025-11-22 10:00:00',60000,65000,'APPROVED');
-- Note: E3 has intentional bad data (DOB in future, discharge before admission) for validation testing.
