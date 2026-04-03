# J2D Healthcare – End-to-End Azure Data Engineering Project

## Project Overview

An **industry-standard healthcare data pipeline** built on **Microsoft Azure** (optimized for Azure Free Trial) demonstrating a complete medallion architecture from raw source ingestion to Power BI dashboards.

| Attribute | Value |
|-----------|-------|
| Domain | Healthcare |
| Architecture | Medallion (Raw → Bronze → Silver → Gold) |
| Cloud | Microsoft Azure (Free Trial compatible) |
| Orchestration | Azure Data Factory |
| Processing | Azure Databricks (PySpark) |
| Storage | Azure Data Lake Storage Gen2 |
| Warehouse | Azure Synapse Analytics (Serverless SQL Pool) |
| Visualization | Power BI Desktop |

---

## Dataset Overview

| File | Source System | Records | Domain |
|------|---------------|---------|--------|
| `Hospital_events_January_2026.csv` | Azure MySQL Flexible Server | ~1,000 | Patient admissions, billing, claims |
| `Device_events_January_2026.csv` | Azure SQL Database | 50 | Medical device utilization per hospital |
| `pharma_events` (table) | Azure Database for PostgreSQL | 100 | Medicine stock and expiry tracking |

### Hospital Events Schema
| Column | Type | Description |
|--------|------|-------------|
| event_id | STRING | Unique admission event ID |
| patient_id | STRING | Patient identifier |
| patient_gender | STRING | M / F / O |
| patient_dob | DATE | Date of birth |
| insurance_id | STRING | Insurance provider ID |
| insurance_name | STRING | Insurance company name |
| department | STRING | Cardiology, Neurology, etc. |
| doctor_id | STRING | Attending doctor |
| bed_id | STRING | Bed assigned |
| bed_type | STRING | ICU / General / Private |
| hospital_id | STRING | H1–H10 |
| admission_time | TIMESTAMP | Admission datetime |
| discharge_time | TIMESTAMP | Discharge datetime (NULL if still admitted) |
| billed_amount | INT | Total bill raised |
| approved_amount | INT | Insurance approved amount |
| claim_status | STRING | APPROVED / REJECTED / PENDING |

---

## Architecture

```
╔══════════════════════════════════════════════════════════════╗
║              Azure Resource Group: rg-j2d-healthcare         ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  [Source Systems]                                            ║
║  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐   ║
║  │Azure MySQL  │  │Azure SQL DB  │  │Azure PostgreSQL   │   ║
║  │(Hospital)   │  │(Device)      │  │(Pharma)           │   ║
║  └──────┬──────┘  └──────┬───────┘  └─────────┬─────────┘   ║
║         │                │                    │              ║
║         └────────────────┼────────────────────┘              ║
║                          │                                   ║
║              [Azure Data Factory]                            ║
║         PL_J2D_HealthcareIngest (Weekly Trigger)             ║
║                          │                                   ║
║                          ▼                                   ║
║         ADLS Gen2 – rawlayer/Raw_Data/ (CSV)                 ║
║                          │                                   ║
║              [Databricks – Raw_NB.py]                        ║
║         Reads rawlayer CSVs, validates schema/nulls/types   ║
║         Writes validated Parquet → bronzelayer               ║
║                          │                                   ║
║                          ▼                                   ║
║         ADLS Gen2 – bronzelayer/Staging_Data/ (Parquet)      ║
║                          │                                   ║
║              [Databricks – Silver_NB.py]                     ║
║         Cleanses data, builds 6-dim + 4-fact Delta tables   ║
║                          │                                   ║
║                          ▼                                   ║
║         Databricks Delta Tables (silver schema)              ║
║         6 dim tables + 4 fact tables                         ║
║                          │                                   ║
║              [Databricks – Gold_NB.py]                       ║
║         7 Gold aggregation tables → written to ADLS          ║
║                          │                                   ║
║                          ▼                                   ║
║         ADLS Gen2 – goldlayer/Gold_Data/ (Parquet)           ║
║                          │                                   ║
║              [Azure Synapse – Serverless SQL]                ║
║         External tables + Power BI views                     ║
║                          │                                   ║
║                          ▼                                   ║
║              [Power BI Desktop]                              ║
║         4-page dashboard with 20+ visuals                    ║
╚══════════════════════════════════════════════════════════════╝
```


---

## Repository Structure

```
J2D_Healthcare_Project/
├── adf_assets/
│   ├── linkedservice/                    # Azure connection endpoints
│   └── pipeline/                         # ADF pipeline pipelines & triggers
│
├── database_scripts/
│   ├── azuresql_device_setup.sql         # Azure SQL DDL + source inserts
│   ├── mysql_hospital_setup.sql          # MySQL DDL + source inserts
│   ├── postgresql_pharma_setup.sql       # PostgreSQL DDL + source inserts
│   ├── synapse_external_tables.sql       # Synapse external tables (Serverless)
│   └── synapse_gold_views.sql            # Power BI-ready views
│
├── databricks_notebooks/
│   ├── Create_Tables.py                  # Silver Delta table DDL
│   ├── Helper_NB.py                      # Shared utility functions
│   ├── Raw_NB.py                         # Raw → Bronze layer notebook
│   ├── Silver_NB.py                      # Bronze → Silver layer notebook
│   └── Gold_NB.py                        # Silver → Gold layer notebook
│
├── documentation/
│   ├── Architecture_and_Runbook.md               # Step-by-step setup guide
│   ├── Team_Roles_and_Tasks.md                   # Role-based task breakdown
│   └── (Other guides, PPTs, HTML tools)          # Checklists and documentation
│
├── logic_apps/
│   └── LA_PipelineFailure_Email.json     # Logic workflow for error alerts
│
├── sample_data/
│   ├── Hospital_events_January_2026.csv
│   ├── Device_events_January_2026.csv
│   └── Pharma_events_January_2026.csv
│
└── README.md                             # This overview file
```

---

## Silver Layer – Data Model

```
          dim_patient          dim_insurance
              │                     │
              └──────┬──────────────┘
                     │
              fact_admission ─── fact_billing
                     │
              dim_doctor   dim_bed   dim_device
                                          │
                                   fact_device_usage
              
              dim_medicine
                   │
           fact_medicine_inventory
```

---

## Gold Layer – Business Outputs

| Gold Table | Business Question |
|------------|-------------------|
| `gold_medicine_availability` | Which hospital has low/critical medicine stock? |
| `gold_device_availability` | Are ICU devices overloaded in any hospital? |
| `gold_finance_weekly` | Weekly revenue leakage and approval % by insurer |
| `gold_weekly_admissions` | Weekly patient admission trend |
| `gold_claim_status_summary` | APPROVED/REJECTED/PENDING split per insurer |
| `gold_rejected_claims` | Top insurers by rejection count |
| `gold_patient_status` | Currently admitted vs discharged count |

---

## Azure Free Trial Cost Estimate

| Service | SKU | Estimated Cost |
|---------|-----|----------------|
| Azure MySQL Flexible Server | Burstable B1ms (1 vCore, 2 GB) | ~$13/month |
| Azure SQL Database | Serverless GP (auto-pause) | ~$5/month (minimal use) |
| Azure Database for PostgreSQL | Burstable B1ms (1 vCore, 2 GB) | ~$13/month |
| ADLS Gen2 | LRS, < 5 GB | ~$0.5/month |
| Azure Data Factory | ~4 weekly pipeline runs/month | ~$2/month |
| Azure Databricks | Single Node, auto-terminate 15min | ~$2–5/run |
| Azure Synapse (Serverless) | Pay per TB scanned (< 1 TB) | ~$1/month |
| **Total** | | **~$35–43/month** |

> 💡 All covered under the **$200 free trial credit** – approximate total for 1 month is well within limits.

---

## Power BI Dashboard – Page Layout

### Page 1: Hospital Overview
- KPI Cards: Total Admissions, Currently Admitted, Discharged
- Line Chart: Weekly Admissions Trend
- Bar Chart: Admissions by Department

### Page 2: Finance & Claims
- KPI Cards: Total Billed, Total Approved, Revenue Leakage
- Column Chart: Revenue Leakage by Insurer
- Table: Weekly Claims by Insurer with Approval %
- Donut Chart: Claim Status Distribution

### Page 3: Device Utilization
- Matrix: Utilization % by Hospital × Device Type (conditional formatting)
- Bar Chart: Available vs In-Use Units by Device Type
- Alert Table: Overloaded devices (>90% utilization)

### Page 4: Pharma Inventory
- Bar Chart: Available Stock by Medicine Category
- Table: Low/Critical Stock Alert per Hospital
- Card: Expired Medicines Count
- Scatter: Total Stock vs Available Stock by Medicine

---

## Audit Trail

Every notebook writes to `J2D_Audit_table` (Databricks Delta) with:
- `pipeline_name`, `notebook_name`, `run_id`, `source`, `layer`
- `record_count`, `load_time`, `status`, `report_month`

Query anytime:
```sql
SELECT * FROM J2D_Audit_table ORDER BY load_time DESC;
```
