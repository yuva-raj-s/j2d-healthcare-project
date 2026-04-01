-- ============================================================
-- J2D Healthcare Project
-- Synapse Gold Views – Power BI Ready
-- Run AFTER synapse_external_tables.sql
-- These views add business-friendly column names and filters
-- for direct use in Power BI via DirectQuery
-- ============================================================

USE j2d_gold_db;
GO

-- ============================================================
-- VIEW 1: vw_Medicine_Stock_Alert
-- Shows medicines with low available stock (< 20% remaining)
-- ============================================================
CREATE OR ALTER VIEW gold.vw_Medicine_Stock_Alert AS
SELECT
    hospital_id,
    medicine_name,
    category,
    available_stock,
    expiry_date,
    report_month,
    CASE
        WHEN available_stock < 200   THEN 'CRITICAL'
        WHEN available_stock < 500   THEN 'LOW'
        ELSE 'ADEQUATE'
    END AS stock_alert_level
FROM gold.medicine_availability;
GO

-- ============================================================
-- VIEW 2: vw_Device_Utilization
-- Shows utilization % and alert for each device type per hospital
-- ============================================================
CREATE OR ALTER VIEW gold.vw_Device_Utilization AS
SELECT
    hospital_id,
    device_type,
    total_units,
    in_use_units,
    available_units,
    ROUND(utilization_pct, 1) AS utilization_pct,
    report_month,
    CASE
        WHEN utilization_pct >= 90 THEN 'OVERLOADED'
        WHEN utilization_pct >= 70 THEN 'HIGH'
        WHEN utilization_pct >= 50 THEN 'MODERATE'
        ELSE 'AVAILABLE'
    END AS utilization_status
FROM gold.device_availability;
GO

-- ============================================================
-- VIEW 3: vw_Finance_Summary
-- Weekly finance rollup with approval rate
-- ============================================================
CREATE OR ALTER VIEW gold.vw_Finance_Summary AS
SELECT
    week_start_date,
    DATEPART(WEEK, week_start_date)         AS week_number,
    insurance_id,
    insurance_name,
    total_claims,
    ROUND(total_billed_amount, 0)           AS total_billed,
    ROUND(total_approved_amount, 0)         AS total_approved,
    ROUND(revenue_leakage_amount, 0)        AS revenue_leakage,
    approval_percentage,
    report_month
FROM gold.finance_weekly;
GO

-- ============================================================
-- VIEW 4: vw_Weekly_Admissions
-- Admission trend over weeks
-- ============================================================
CREATE OR ALTER VIEW gold.vw_Weekly_Admissions AS
SELECT
    week_start_date,
    DATEPART(WEEK, week_start_date)  AS week_number,
    DATENAME(MONTH, week_start_date) AS month_name,
    DATEPART(YEAR, week_start_date)  AS year_number,
    total_admissions,
    report_month
FROM gold.weekly_admissions;
GO

-- ============================================================
-- VIEW 5: vw_Claim_Status_Breakdown
-- Claim status pivot-ready for donut/bar charts
-- ============================================================
CREATE OR ALTER VIEW gold.vw_Claim_Status_Breakdown AS
SELECT
    insurance_name,
    claim_status,
    claim_count,
    report_month,
    SUM(claim_count) OVER (PARTITION BY insurance_name) AS total_claims_per_insurer,
    ROUND(
        100.0 * claim_count / 
        SUM(claim_count) OVER (PARTITION BY insurance_name), 1
    ) AS claim_status_pct
FROM gold.claim_status_summary;
GO

-- ============================================================
-- VIEW 6: vw_Rejected_Claims_Ranking
-- Ranked insurers by rejection count
-- ============================================================
CREATE OR ALTER VIEW gold.vw_Rejected_Claims_Ranking AS
SELECT
    insurance_name,
    rejected_claim_count,
    report_month,
    RANK() OVER (ORDER BY rejected_claim_count DESC) AS rejection_rank
FROM gold.rejected_claims;
GO

-- ============================================================
-- VERIFY: Quick preview of all views
-- ============================================================
SELECT TOP 5 * FROM gold.vw_Medicine_Stock_Alert;
SELECT TOP 5 * FROM gold.vw_Device_Utilization;
SELECT TOP 5 * FROM gold.vw_Finance_Summary;
SELECT TOP 5 * FROM gold.vw_Weekly_Admissions;
SELECT TOP 5 * FROM gold.vw_Claim_Status_Breakdown;
SELECT TOP 5 * FROM gold.vw_Rejected_Claims_Ranking;
