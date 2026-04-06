# J2D Healthcare – Step-by-Step Architecture & Runbook

## Phase 1: Azure Resource Creation

### 1.1 Create Resource Group
```
Portal > Resource Groups > + Create
- Name      : rg-j2d-healthcare
- Region    : East US (or your preferred region)
- Tags      : Project=J2D-Healthcare, Environment=Dev
```

---

### 1.2 Create ADLS Gen2 Storage Account (Already exists: j2dstorage101)

**Add `goldlayer` container:**
```
Portal > j2dstorage101 > Containers > + Container
- Name              : goldlayer
- Public access     : Private
```

Ensure these containers exist:
| Container | Purpose |
|-----------|---------|
| `rawlayer` | Landing zone – CSV files from ADF |
| `bronzelayer` | Validated Parquet (Raw_NB output) |
| `goldlayer` | Aggregated Parquet (Gold_NB output) |

---

### 1.3 Create Azure MySQL Flexible Server (Hospital Source)

```
Portal > + Create > Azure Database for MySQL – Flexible Server
- Server name    : j2d-mysql-server
- Resource group : rg-j2d-healthcare
- Region         : East US
- MySQL version  : 8.0
- Workload type  : Development (cheapest)
- SKU            : Burstable, B1ms (1 vCore, 2 GB RAM)
- Storage        : 20 GB (minimum)
- Backup         : 7 days, Locally Redundant
- Admin username : j2dadmin
- Password       : J2D@Mysql2026!
- Connectivity   : Public access (select "Allow public access from any Azure service")
```

After creation:
1. Go to **Networking** → Add firewall rule for your IP + azure services
2. Open MySQL Workbench or Azure Cloud Shell
3. Run `SQL_Scripts/mysql_hospital_setup.sql`

---

### 1.4 Create Azure SQL Database (Device Source)

```
Portal > + Create > SQL Database
- Server name        : j2d-sqlserver (new server)
- Database name      : j2d_device_db
- Resource group     : rg-j2d-healthcare
- Compute + storage  : Configure > Serverless > General Purpose
                       vCores: 1 min / 1 max
                       Auto-pause: 60 min
                       Data max: 32 GB
- Backup redundancy  : Locally Redundant
- Admin login        : j2dadmin
- Password           : J2D@Sql2026!
- Connectivity       : Public endpoint, Allow Azure services = YES
```

After creation:
1. Go to **Query editor** in Portal
2. Run `SQL_Scripts/azuresql_device_setup.sql`

---

### 1.5 Create Azure Database for PostgreSQL (Pharma Source)

```
Portal > + Create > Azure Database for PostgreSQL – Flexible Server
- Server name    : j2d-pg-server
- Resource group : rg-j2d-healthcare
- Region         : East US
- Workload type  : Development
- Compute + store: Burstable, B1ms (1 vCore, 2 GB RAM)
- Admin username : j2dadmin
- Password       : J2D@Pg2026!
- Connectivity   : Public access (allow Azure services)
```

After creation:
1. Go to **Networking** → Add firewall rule for your IP + azure services
2. Open pgAdmin or Azure Cloud Shell (psql)
3. Run `SQL_Scripts/postgresql_pharma_setup.sql`

---

### 1.6 Create Azure Data Factory

```
Portal > + Create > Data Factory
- Name           : j2d-adf-healthcare
- Resource group : rg-j2d-healthcare
- Region         : East US
- Version        : V2
```

---

### 1.7 Create Azure Synapse Analytics Workspace

```
Portal > + Create > Azure Synapse Analytics
- Workspace name   : j2d-synapse-101
- Resource group   : rg-j2d-healthcare
- Region           : East US
- Data Lake Storage: Select j2dstorage101, filesystem = goldlayer
- SQL admin        : j2dadmin
- Password         : J2D@Synapse2026!
```

> **DO NOT** create a Dedicated SQL Pool – use only the built-in **Serverless SQL Pool** (free per TB scanned).

**Grant Synapse Managed Identity access to ADLS:**
```
Portal > j2dstorage101 > Access Control (IAM)
> + Add role assignment
> Role: Storage Blob Data Reader
> Assign to: Managed Identity > j2d-synapse-101
```

---

## Phase 2: Azure Data Factory Setup

### 2.1 Create Linked Services in ADF

Navigate to: **ADF Studio > Manage > Linked Services**

#### A) MySQL Linked Service
```
+ New > Azure Database for MySQL
- Name              : LS_MySQL_Hospital
- Server name       : j2d-mysql-server.mysql.database.azure.com
- Database name     : j2d_hospital_db
- Username          : j2dadmin@j2d-mysql-server
- Password          : J2D@Mysql2026!
- Test connection   : ✓ must pass
```

#### B) Azure SQL Linked Service
```
+ New > Azure SQL Database
- Name              : LS_AzureSQL_Device
- Server name       : j2d-sqlserver.database.windows.net
- Database name     : j2d_device_db
- Authentication    : SQL Authentication
- Username          : j2dadmin
- Password          : J2D@Sql2026!
- Test connection   : ✓ must pass
```

#### C) Azure PostgreSQL Linked Service (Pharma source)
```
+ New > Azure Database for PostgreSQL
- Name              : LS_PostgreSQL_Pharma
- Server name       : j2d-pg-server.postgres.database.azure.com
- Database name     : j2d_pharma_db
- Username          : j2dadmin
- Password          : J2D@Pg2026!
- Test connection   : ✓ must pass
```

#### D) ADLS Gen2 Linked Service (Raw layer sink)
```
+ New > Azure Data Lake Storage Gen2
- Name              : LS_ADLS_RawLayer
- Storage account   : j2dstorage101
- Test connection   : ✓ must pass
```

#### E) Databricks Linked Service
```
+ New > Azure Databricks
- Name              : LS_AzureDatabricks
- Databricks workspace : Select your existing workspace
- Cluster           : New job cluster (cost-efficient)
  - Cluster version : 13.3 LTS (Spark 3.4, Scala 2.12)
  - Node type       : Standard_DS3_v2 (minimum)
  - Single node     : YES (free trial – saves cost)
  - Auto-terminate  : 15 minutes
- Authentication    : Managed Service Identity or Personal Access Token
```

---

### 2.2 Create Datasets in ADF

Navigate to: **ADF Studio > Author > Datasets**

| Dataset Name | Linked Service | Type |
|---|---|---|
| `DS_MySQL_HospitalEvents` | LS_MySQL_Hospital | MySQL table: `hospital_events` |
| `DS_AzureSQL_DeviceEvents` | LS_AzureSQL_Device | Azure SQL table: `dbo.device_events` |
| `DS_PostgreSQL_PharmaEvents` | LS_PostgreSQL_Pharma | PostgreSQL table: `pharma_events` |
| `DS_ADLS_Raw_Hospital` | LS_ADLS_RawLayer | DelimitedText: `rawlayer/Raw_Data/Hospital_events_January_2026.csv` |
| `DS_ADLS_Raw_Device` | LS_ADLS_RawLayer | DelimitedText: `rawlayer/Raw_Data/Device_events_January_2026.csv` |
| `DS_ADLS_Raw_Pharma` | LS_ADLS_RawLayer | DelimitedText: `rawlayer/Raw_Data/Pharma_events_January_2026.csv` |

---

### 2.3 Import Pipeline

```
ADF Studio > Author > Pipelines > ⋮ > Import from pipeline JSON
Upload: ADF_Pipeline_JSON/PL_J2D_HealthcareIngest.json
```

Update linked service references if they don't auto-resolve.

---

### 2.4 Create Weekly Schedule Trigger

> **Updated for testing:** Weekly trigger fires every Monday at 6:00 AM IST, so you can observe automated runs within days rather than waiting a full month.

```
ADF Studio > Author > Pipelines > PL_J2D_HealthcareIngest
> Add Trigger > New/Edit
- Name          : TRG_Weekly_Healthcare_Ingest
- Type          : Schedule
- Start date    : 2026-04-07 (next Monday)
- Time zone     : India Standard Time
- Recurrence    : Every 1 Week(s) on Monday at 06:00 AM
- Parameters    : input_date = @formatDateTime(trigger().scheduledTime,'yyyyMMdd')
- End           : No end date
```

> 💡 **UTC Note:** 6:00 AM IST = 00:30 AM UTC. ADF stores times in UTC internally, but when you select "India Standard Time" in the UI it converts automatically.

Click **Publish All** after creating the trigger to activate it.

---

## Phase 7: Trigger Testing – Manual & Automatic

This phase covers how to test the pipeline in both modes — exactly as done in real teams before going to production.

---

### 7.1 Manual Trigger – ADF (Run Now)

Use this to validate the pipeline on demand **without waiting for the schedule**.

#### Step-by-Step

```
ADF Studio > Author > Pipelines > PL_J2D_HealthcareIngest

Step 1: Click "Add Trigger" > "Trigger Now"

Step 2: In the pop-up, enter the pipeline parameter:
  - input_date : 20260201
    (Format: YYYYMMDD — this means the run processes January 2026 data)

Step 3: Click OK → the pipeline starts immediately

Step 4: Navigate to Monitor > Pipeline runs
  - You will see PL_J2D_HealthcareIngest with Status: In Progress
  - Click on the row to see individual activity runs:
      ✓ Copy_Hospital_From_MySQL     → should show rows copied
      ✓ Copy_Device_From_AzureSQL   → should show rows copied
      ✓ Copy_Pharma_From_PostgreSQL → should show rows copied
      ✓ Run_Raw_NB                  → Databricks run link
      ✓ Run_Silver_NB               → Databricks run link
      ✓ Run_Gold_NB                 → Databricks run link
```

#### What to Check After Manual Run

```
1. ADLS rawlayer:
   Portal > j2dstorage101 > rawlayer > Raw_Data > verify 3 CSVs exist

2. ADLS bronzelayer:
   bronzelayer > Staging_Data > Hospital_Data/
                              > Device_Data/
                              > Pharma_Data/
   → Each should contain a .parquet file

3. ADLS goldlayer:
   goldlayer > Gold_Data > gold_medicine_availability/
                         > gold_device_availability/
                         > gold_finance_weekly/
                         > gold_weekly_admissions/
                         > gold_claim_status_summary/
                         > gold_rejected_claims/
   → Each should contain a .parquet file

4. Databricks Audit table:
   Run in any Databricks notebook:
   display(spark.sql("SELECT * FROM J2D_Audit_table ORDER BY load_time DESC"))

5. Synapse (after Gold files exist):
   SELECT COUNT(*) FROM gold.medicine_availability;
```

---

### 7.2 Manual Trigger – Databricks Workflow (Run Now)

If you want to test notebooks independently without ADF:

```
Databricks > Workflows > J2D_Healthcare_Weekly_Job

Step 1: Click "Run Now" (top right)

Step 2: In the pop-up, override parameters if needed:
  - input_date : 20260201

Step 3: Click Run
  - Job status shows: Pending → Running → Success/Failed
  - Click each task to see notebook output, logs, Spark UI

Step 4: After completion check the Run History tab
  - Duration, start/end time, exit value from each notebook
```

---

### 7.3 Automatic Trigger – Weekly ADF Schedule

The weekly trigger `TRG_Weekly_Healthcare_Ingest` fires automatically every Monday at 6:00 AM IST.

#### Verify the Trigger Is Active

```
ADF Studio > Manage > Triggers
- TRG_Weekly_Healthcare_Ingest should show Status: Started
  (if it shows "Stopped", click the toggle to activate)
```

#### How to Read Trigger Runs

```
ADF Studio > Monitor > Trigger runs
- Filter by Trigger name: TRG_Weekly_Healthcare_Ingest
- Each row = one scheduled fire
  - Status: Succeeded = pipeline was kicked off
  - Click the pipeline run link to see execution details
```

#### Trigger Parameter Behaviour

| Trigger | input_date value | Which data it processes |
|---------|-----------------|------------------------|
| Manual (Trigger Now) | You enter: `20260201` | January 2026 |
| Auto – Monday Apr 7 | Auto: `20260407` | March 2026 (prev month) |
| Auto – Monday Apr 14 | Auto: `20260414` | March 2026 (prev month) |

> The `input_date` passed to the notebook is always `trigger().scheduledTime`. Inside the notebooks, `prev_month_full_name` is calculated as 1 month before that date — this is correct for processing the previous month's data.

---

### 7.4 Automatic Trigger – Databricks Workflow (Weekly)

Set up a weekly schedule directly in Databricks as backup to ADF:

```
Databricks > Workflows > J2D_Healthcare_Weekly_Job (or create new)
> Edit > Schedule

- Cron expression : 0 30 0 * * MON   (every Monday 00:30 UTC = 6:00 AM IST)
- OR use UI       : Weekly, Monday, 06:00 AM, India/Kolkata timezone

Parameters (static or computed):
  - input_date : Use a dynamic value via a Python task that computes:
      from datetime import datetime
      date_str = datetime.now().strftime('%Y%m%d')
```

> **Industry Note:** In real teams, ADF is the primary orchestrator and Databricks Workflow is secondary. Only one should own the schedule — the other is a manual fallback.

---

### 7.5 Trigger Comparison Table

| Feature | ADF Manual (Trigger Now) | ADF Weekly Schedule | Databricks Job Manual | Databricks Weekly Job |
|---------|--------------------------|---------------------|-----------------------|----------------------|
| **Who initiates** | You (human) | Azure scheduled event | You (human) | Azure scheduled event |
| **When to use** | Development & testing | Production auto-run | Notebook-only testing | Backup automation |
| **input_date** | You enter manually | Auto from trigger time | You enter in pop-up | Static or computed |
| **Monitoring** | ADF Monitor > Pipeline runs | ADF Monitor > Trigger runs | Databricks > Job Runs | Databricks > Job Runs |
| **Audit trail** | J2D_Audit_table ✓ | J2D_Audit_table ✓ | J2D_Audit_table ✓ | J2D_Audit_table ✓ |
| **Cost** | Credits used per run | Credits used per run | Credits used per run | Credits used per run |

---

### 7.6 Manual Trigger Test Checklist

Run through this checklist each time you do a manual test run:

```
PRE-RUN:
[ ] Confirm all 3 Linked Services test connection ✓
[ ] Confirm all 6 Datasets are saved and published
[ ] Confirm Databricks cluster config (Single Node, auto-terminate 15 min)
[ ] input_date parameter format is YYYYMMDD (e.g., 20260201)

DURING RUN (Monitor > Pipeline runs):
[ ] All 3 Copy activities show green ✓
[ ] Run_Raw_NB shows Succeeded (exit value: "All validations are successfully passed")
[ ] Run_Silver_NB shows Succeeded
[ ] Run_Gold_NB shows Succeeded (exit value: "Gold layer processing completed...")

POST-RUN:
[ ] rawlayer: 3 CSV files present
[ ] bronzelayer: 3 Parquet folders present
[ ] goldlayer: 7 Parquet folders present
[ ] Synapse: SELECT COUNT(*) > 0 for at least one external table
[ ] Audit table: 10+ new rows with status = 'Success'
[ ] Power BI: Refresh shows updated data
```

---

### 7.7 Simulating Weekly Runs with Different Dates

Since your data is all January 2026, you can simulate multiple "weekly" runs by changing `input_date`:

| Test Run | input_date | Simulates |
|----------|-----------|-----------|
| Run 1 (Manual) | `20260201` | Pipeline run for February 1st → processes January data |
| Run 2 (Manual) | `20260208` | Pipeline run for February 8th → processes January data |
| Run 3 (Auto) | Auto on Mon Apr 7 | Pipeline run for Apr 7 → processes March data |

> **For demo purposes:** You can run manually with `20260201` multiple times — the pipeline uses `mode("overwrite")` so each run refreshes the Gold data cleanly.

---

## Phase 3: Upload Notebooks to Databricks

### 3.1 Import Notebooks

```
Databricks Workspace > Workspace > Users > your-email > Import
```

Import each `.py` file (select "Source Files (.py)"):
1. `Helper_NB.py`
2. `Create table.py` → Run once to create Silver Delta tables
3. `Raw_NB.py`
4. `Silver_NB.py`
5. `Gold_NB.py`

### 3.2 Run Create Table Notebook Once

```
Databricks > Open "Create table" notebook > Run All
```
This creates all Silver Delta tables (dim + fact).

---

## Phase 4: Databricks Job (Alternative to ADF Trigger)

Create a Databricks Job for standalone execution (alternative/backup to ADF):

```
Databricks > Workflows > Create Job

- Name: J2D_Healthcare_Weekly_Job

- Task 1 – Raw_NB
  - Type       : Notebook
  - Notebook   : /Workspace/Users/kattayashu2020@gmail.com/Raw_NB
  - Parameters : input_date = 20260201   ← override at runtime
  - Depends on : (none – first task)

- Task 2 – Silver_NB
  - Type       : Notebook
  - Notebook   : /Workspace/Users/kattayashu2020@gmail.com/Silver_NB
  - Parameters : input_date = 20260201
  - Depends on : Raw_NB (Success)

- Task 3 – Gold_NB
  - Type       : Notebook
  - Notebook   : /Workspace/Users/kattayashu2020@gmail.com/Gold_NB
  - Parameters : input_date = 20260201
  - Depends on : Silver_NB (Success)

- Cluster     : Single Node, Standard_DS3_v2, auto-terminate 15 min
- Schedule    : Every Monday at 6:00 AM (Cron: 0 30 0 * * 1)
  OR via UI   : Weekly, Monday, 06:00 AM, Timezone = Asia/Kolkata

- Email on failure : your@email.com
```

**To run manually right now (Run Now):**
```
Workflows > J2D_Healthcare_Weekly_Job > Run Now
> Override parameter: input_date = 20260201
```

---

## Phase 5: Synapse Setup

### 5.1 Open Synapse Studio

```
Portal > j2d-synapse-101 > Open Synapse Studio
```

### 5.2 Run SQL Scripts

```
Synapse Studio > Develop > +SQL Script > Built-in pool

Step 1: Run synapse_external_tables.sql
Step 2: Run synapse_gold_views.sql
```

### 5.3 Verify Data

```sql
-- In Synapse Studio
SELECT TOP 5 * FROM gold.medicine_availability;
SELECT TOP 5 * FROM gold.vw_Finance_Summary;
```

---

## Phase 6: Power BI Desktop

### 6.1 Connect to Synapse

```
Power BI Desktop > Get Data > Azure > Azure Synapse Analytics SQL
- Server: j2d-synapse-101-ondemand.sql.azuresynapse.net
- Database: j2d_gold_db
- Data Connectivity: DirectQuery (recommended) or Import
```

### 6.2 Select Tables/Views

Select these views:
- `gold.vw_Finance_Summary`
- `gold.vw_Weekly_Admissions`
- `gold.vw_Device_Utilization`
- `gold.vw_Medicine_Stock_Alert`
- `gold.vw_Claim_Status_Breakdown`
- `gold.vw_Rejected_Claims_Ranking`

### 6.3 Build Dashboard

**Page 1 – Hospital Overview**
- Card: DISTINCTCOUNT(admission_id) → "Total Admissions"
- Card: COUNTROWS(FILTER(admitted, is_currently=true)) → "Currently Admitted"
- Line Chart: week_start_date vs total_admissions
- Stacked Bar: Department vs Admission Count

**Page 2 – Finance & Claims**
- Card: SUM(revenue_leakage) → "Total Revenue Leakage ₹"
- Card: AVERAGE(approval_percentage) → "Avg Approval %"
- Clustered Bar: insurance_name vs revenue_leakage
- Table: Weekly claims with conditional formatting on approval_pct
- Donut: claim_status distribution

**Page 3 – Device Utilization**
- Matrix: hospital_id (rows) × device_type (cols) → utilization_pct (values)
  - Conditional formatting: Red > 90%, Yellow 70–90%, Green < 70%
- Bar: device_type vs available_units

**Page 4 – Pharma Inventory**
- Bar: category vs total_available_stock
- Table: hospital_id, medicine_name, available_stock, stock_alert_level
  - Color-code: CRITICAL=Red, LOW=Yellow, ADEQUATE=Green
- Card: Count of EXPIRED medicines

---

## Observability & Monitoring

### ADF Monitoring
```
ADF Studio > Monitor > Pipeline runs
```
View: Duration, status, activity-level logs for each run.

### Databricks Job Monitoring
```
Databricks > Workflows > J2D_Healthcare_Weekly_Job > Runs
```

### Audit Table Query
```sql
-- In Databricks SQL or notebook
SELECT pipeline_name, notebook_name, layer, record_count, status, load_time, report_month
FROM J2D_Audit_table
ORDER BY load_time DESC;
```

### ADLS File Verification
```python
# In Databricks notebook
display(dbutils.fs.ls("abfss://goldlayer@j2dstorage101.dfs.core.windows.net/Gold_Data/"))
```

---

## Troubleshooting

| Issue | Resolution |
|-------|-----------|
| ADF Copy fails – MySQL connection | Verify firewall allows Azure services, check username format `j2dadmin@j2d-mysql-server` |
| Databricks notebook fails at ADLS write | Verify `spark.conf.set` with correct account_key or use Key Vault |
| Synapse external table shows no rows | Check Managed Identity role assignment (Storage Blob Data Reader) on ADLS |
| Parquet read error in Synapse | Ensure Gold_NB used `coalesce(1).write.format("parquet")` not Delta format |
| Power BI can't connect | Use "Serverless SQL endpoint" not "Dedicated SQL endpoint" from Synapse properties |
