# CI/CD Setup Guide for J2D Healthcare Project

This repository includes CI/CD pipeline definitions for **both** GitHub Actions and Azure DevOps to give you hands-on experience orchestrating code deployments natively on the cloud.

> [!WARNING]
> Both pipelines assume your **Azure Data Factory** is configured with [Git Integration](https://learn.microsoft.com/en-us/azure/data-factory/source-control). 
> When you link ADF to GitHub or Azure Repos, ADF automatically compiles your JSON files into an ARM template usually pushed to an `adf_publish` branch. 
> Without Git integration, deploying raw ADF JSON arrays via scripts is extremely hacky and avoids industry standard best-practices. Please configure Git Integration from your ADF Studio UI before proceeding.

## Required Secrets & Variables

Before executing either pipeline, you must configure secrets in your respective platforms to allow the CI/CD runner to authenticate securely against your Azure Cloud Services.

### 1. Databricks Authentication
You need to generate a Personal Access Token (PAT) inside your Databricks workspace.
- **`DATABRICKS_HOST`**: The URL of your Databricks instance (eg. `https://adb-123456789.0.azuredatabricks.net`)
- **`DATABRICKS_TOKEN`**: The string starting with `dapi...`

### 2. Azure Authentication
You need an Azure Service Principal (App Registration) with `Contributor` rights to your resource group `rg-j2d-healthcare`. Ensure you generate a client secret associated with that SP.

---

## 🚀 Setup: GitHub Actions
The pipeline YAML is located at `.github/workflows/deploy-azure-data-platform.yml`.

1. Go to your GitHub Repository -> **Settings** -> **Secrets and variables** -> **Actions**.
2. Create the following **New repository secrets**:
   - `DATABRICKS_HOST`
   - `DATABRICKS_TOKEN`
   - `AZURE_CREDENTIALS` 
   
   *(Note: The Azure Credentials secret expects a JSON structure Outputting from the `az ad sp create-for-rbac` CLI command).*

---

## 🚀 Setup: Azure DevOps
The pipeline YAML is located at `azure-pipelines.yml`.

1. Go to your Azure DevOps Project -> **Project Settings** -> **Service connections**.
2. Create a new **Azure Resource Manager** connection named `J2D-Azure-Service-Connection`.
3. Go to **Pipelines** -> **Library**.
4. Create a new **Variable Group** (link it to your pipeline) and securely input the following:
   - `DATABRICKS_HOST`
   - `DATABRICKS_TOKEN`
   - `AZURE_SUBSCRIPTION_ID`

---

## Deployment Logic

Both CI runs trigger sequentially:
1. **Deploy Databricks**: Uses the open-source Databricks CLI to sync all code located inside `Databricks_Notebooks/` to `/Workspace/Shared/J2D_Project` over HTTPS.
2. **Deploy ADF**: Uses Azure standard deployment tasks to read your ARM templates and push changes across Linked Services, Pipelines, and Dataset definitions into the live Azure Data Factory service instance.
