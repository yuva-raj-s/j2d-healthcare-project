-- ============================================================
-- J2D Healthcare Project
-- PostgreSQL Setup Script – Pharma Source (Azure Database for PostgreSQL)
-- Target DB : j2d_pharma_db
-- Table     : pharma_events
-- Run this in pgAdmin or Azure Cloud Shell (psql)
-- ============================================================

-- STEP 1: Create the database
-- CREATE DATABASE j2d_pharma_db;
-- \c j2d_pharma_db

-- STEP 2: Create the pharma_events table
DROP TABLE IF EXISTS pharma_events CASCADE;

CREATE TABLE pharma_events (
    hospital_id           VARCHAR(20)  NOT NULL,
    medicine_id           VARCHAR(20)  NOT NULL,
    medicine_name         VARCHAR(100),
    category              VARCHAR(50),
    total_stock           INT,
    available_stock       INT,
    reorder_level         INT,
    expiry_date           DATE,
    supplier_id           VARCHAR(20),
    report_week           VARCHAR(10),
    PRIMARY KEY (hospital_id, medicine_id)
);

-- STEP 3: Load data (run COPY from CSV via psql or import wizard)
-- COPY pharma_events (hospital_id, medicine_id, medicine_name, category, total_stock, available_stock, reorder_level, expiry_date, supplier_id, report_week)
-- FROM '/path/to/Pharma_events_January_2026.csv'
-- DELIMITER ','
-- CSV HEADER;

-- Option B – Use Azure Data Factory to copy directly (recommended if data is huge)
-- ADF Copy Activity will copy from PostgreSQL to ADLS staging via Linked Service.

-- STEP 4: Create a dedicated ADF user (least-privilege)
-- CREATE USER adf_user WITH PASSWORD 'J2D@Adf2026!';
-- GRANT CONNECT ON DATABASE j2d_pharma_db TO adf_user;
-- GRANT USAGE ON SCHEMA public TO adf_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO adf_user;

-- STEP 5: Verify
-- SELECT COUNT(*) AS total_records FROM pharma_events;
-- SELECT * FROM pharma_events LIMIT 5;

-- ============================================================
-- REFERENCE: Sample INSERT for manual testing (first 3 rows)
-- ============================================================
INSERT INTO pharma_events VALUES
('H1', 'MED001', 'Paracetamol 500mg', 'Analgesic', 5000, 1500, 1000, '2026-12-31', 'SUPP01', 'W1'),
('H1', 'MED002', 'Amoxicillin 250mg', 'Antibiotic', 2000, 400, 500, '2026-06-30', 'SUPP02', 'W1'),
('H2', 'MED003', 'Ibuprofen 400mg', 'Analgesic', 3000, 2500, 800, '2026-10-15', 'SUPP03', 'W1');
-- Note: MED002 available_stock < reorder_level logic validation.
