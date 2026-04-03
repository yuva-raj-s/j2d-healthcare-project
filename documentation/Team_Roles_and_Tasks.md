# J2D Healthcare Project – Team Roles & Task Breakdown

## Overview

Although you are doing this solo, the project is designed to mimic how a **real data engineering team** divides work across sprints. Understanding these boundaries gives you industry exposure on how responsibilities are separated in actual organisations.

---

## Team Structure

```
┌─────────────────────────────────────────────────────────┐
│              J2D Healthcare Data Team                   │
├──────────────┬──────────────┬───────────────┬───────────┤
│  Data Eng    │  Data Eng    │  DWH Eng      │  BI Dev   │
│  (Ingestion) │  (Pipeline)  │  (Synapse)    │  (PowerBI)│
└──────────────┴──────────────┴───────────────┴───────────┘
                      │
              ┌───────┴────────┐
              │  DevOps / Ops  │
              │  (ADF Triggers │
              │   + Monitoring)│
              └────────────────┘
```

---

## Role 1: Data Engineer – Ingestion (Sprint 1)

**Owns:** Source system setup, ADF Linked Services, Datasets, Copy pipelines

### Tasks
- [ ] Create MySQL Flexible Server → create `j2d_hospital_db` database and `hospital_events` table
- [ ] Create Azure SQL Database → create `j2d_device_db` and `dbo.device_events` table
- [ ] Create Azure Database for PostgreSQL → create `j2d_pharma_db` and `pharma_events` table
- [ ] Insert / load sample data into MySQL, Azure SQL, and PostgreSQL
- [ ] Create ADF Linked Services for all 3 sources + ADLS sink
- [ ] Create ADF Datasets (6 total – 3 source, 3 sink)
- [ ] Build Copy Activities (3 parallel in pipeline) and validate with test runs
- [ ] Document Linked Service credentials in Key Vault (do not hardcode in ADF)

**Deliverable:** Working ADF Copy that puts all 3 CSVs into `rawlayer/Raw_Data/`

**Tools:** Azure Portal, MySQL Workbench / SSMS, ADF Studio

---

## Role 2: Data Engineer – Processing (Sprint 2)

**Owns:** Databricks notebooks – Raw (Bronze), Silver layers

### Tasks
- [ ] Import `Helper_NB.py`, `Create table.py`, `Raw_NB.py`, `Silver_NB.py` into Databricks Workspace
- [ ] Run `Create table.py` once to bootstrap Silver schema tables
- [ ] Test `Raw_NB.py` manually with `input_date=20260201`
  - Validate schema checks pass
  - Validate Bronze Parquet files appear in `bronzelayer/Staging_Data/`
- [ ] Test `Silver_NB.py` manually
  - Verify all 7 dimension tables and 4 fact tables populated in Databricks catalog
- [ ] Review and fix the `check_column_is_int` function logic (cosmetic bug in Helper_NB)
- [ ] Ensure `J2D_Audit_table` receives records for both `raw` and `silver` layers

**Deliverable:** Bronze Parquet + Silver Delta tables populated, Audit table logging confirmed

**Tools:** Databricks Workspace, Azure Storage Explorer

---

## Role 3: Data Engineer – Gold Layer (Sprint 3)

**Owns:** Gold_NB.py – Gold aggregations, ADLS Parquet writes

### Tasks
- [ ] Import updated `Gold_NB.py` (already updated in this project) into Databricks
- [ ] Test Gold_NB manually with `input_date=20260201`
- [ ] Verify all 7 Gold Parquet folders exist under `goldlayer/Gold_Data/`
  ```
  goldlayer/Gold_Data/gold_medicine_availability/
  goldlayer/Gold_Data/gold_device_availability/
  goldlayer/Gold_Data/gold_finance_weekly/
  goldlayer/Gold_Data/gold_weekly_admissions/
  goldlayer/Gold_Data/gold_claim_status_summary/
  goldlayer/Gold_Data/gold_rejected_claims/
  ```
- [ ] Confirm row counts match Silver fact table counts
- [ ] Write audit records for Gold layer to `J2D_Audit_table`

**Deliverable:** 7 Gold Parquet folders in ADLS goldlayer, Audit entries for `layer=gold`

**Tools:** Databricks Workspace, Azure Storage Explorer

---

## Role 4: Data Warehouse Engineer (Sprint 4)

**Owns:** Synapse Analytics – external tables, views, SQL optimisation

### Tasks
- [ ] Create Synapse Analytics Workspace (`j2d-synapse`)
- [ ] Assign Synapse Managed Identity → Storage Blob Data Reader on `j2dstorage04`
- [ ] In Synapse Studio → Run `synapse_external_tables.sql` (creates DB, credential, data source, external tables)
- [ ] Run `synapse_gold_views.sql` (creates Power BI views with business logic)
- [ ] Validate all external tables return rows
- [ ] Document Synapse serverless endpoint URL for BI team:
  - Format: `j2d-synapse-ondemand.sql.azuresynapse.net`

**Deliverable:** 6 external tables + 6 views queryable in Synapse Serverless pool

**Tools:** Synapse Studio, Azure Portal (IAM)

---

## Role 5: BI Developer (Sprint 5)

**Owns:** Power BI Desktop – report design, data model, visualisations

### Tasks
- [ ] Connect Power BI Desktop to Synapse Serverless SQL endpoint
- [ ] Load 6 Gold views (DirectQuery mode)
- [ ] Build Page 1 – Hospital Overview
  - Line chart: Weekly Admissions Trend
  - Cards: Total Admissions, Currently Admitted, Discharged
  - Bar: Admissions by Department
- [ ] Build Page 2 – Finance & Claims
  - Cards: Total Billed ₹, Revenue Leakage ₹, Avg Approval %
  - Bar: Revenue leakage by insurer
  - Table: Weekly claims with conditional formatting
  - Donut: Claim status (Approved / Rejected / Pending)
- [ ] Build Page 3 – Device Utilization
  - Matrix with conditional formatting (Red/Yellow/Green)
  - Alert Table: Devices > 90% utilization
- [ ] Build Page 4 – Pharma Inventory
  - Bar: Stock by category
  - Table: Stock alert level per hospital+medicine
  - Card: Expired stock count
- [ ] Apply consistent theme (corporate blue/healthcare palette)
- [ ] Export as `.pbix` file and save to `documentation/`

**Deliverable:** 4-page Power BI report connected to live Synapse data

**Tools:** Power BI Desktop (free download from Microsoft)

---

## Role 6: DevOps / Operations Engineer (Sprint 6)

**Owns:** ADF Trigger, Databricks Job, Monitoring, Alerting

### Tasks
- [ ] Create ADF Weekly Schedule Trigger `TRG_Weekly_Healthcare_Ingest`
  - Schedule: Every Monday at 6:00 AM IST
  - Parameter: `input_date` = `@formatDateTime(trigger().scheduledTime,'yyyyMMdd')`
- [ ] Test Manual Trigger: ADF > Trigger Now with `input_date=20260201`
- [ ] Create Databricks Workflow `J2D_Healthcare_Weekly_Job`
  - Tasks: Raw_NB → Silver_NB → Gold_NB (dependent chain)
  - Cluster: Single Node, auto-terminate 15 min
  - Schedule: Weekly on Monday (backup to ADF trigger)
- [ ] Set up ADF Alert (Azure Monitor):
  - Alert on: Pipeline Run Failure
  - Action: Email to team
- [ ] Enable Databricks job email notifications on failure
- [ ] Document runbook: "What to do when pipeline fails"

**Deliverable:** Automated weekly pipeline, manual trigger tested, alerts configured, runbook written

**Tools:** ADF Studio (Trigger), Databricks Workflows, Azure Monitor

---

## Sprint Timeline (Solo Practice Plan)

| Week | Sprint | Focus |
|------|--------|-------|
| Week 1 | Sprint 1 | Resource creation, Source systems, ADF Copy |
| Week 1 | Sprint 2 | Databricks notebooks – Raw & Silver |
| Week 2 | Sprint 3 | Gold_NB – Gold layer and ADLS writes |
| Week 2 | Sprint 4 | Synapse external tables and views |
| Week 3 | Sprint 5 | Power BI dashboard (4 pages) |
| Week 3 | Sprint 6 | Triggers, Jobs, Monitoring |

---

## Industry Practices Demonstrated

| Practice | Where Used |
|----------|-----------|
| Medallion Architecture (Bronze/Silver/Gold) | Databricks notebooks |
| Parameterized Pipelines | ADF `input_date` parameter |
| Audit logging | `J2D_Audit_table` in all notebooks |
| Schema validation (fail fast) | Raw_NB with `dbutils.notebook.exit()` |
| Least-privilege access | Separate ADF users per source |
| External data sources | Synapse serverless external tables |
| Cost optimization | Auto-pause SQL, auto-terminate Databricks |
| Separation of concerns | 6 role-based responsibilities |
| Source control ready | All code as `.py` and `.sql` files |
| Infrastructure as code | ADF pipeline JSON importable |
