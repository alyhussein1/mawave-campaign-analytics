# Campaign Performance Dashboard --> dbt Project

A multi-layered dbt data product that unifies advertising metrics, internal costs, and resource allocation to provide true campaign profitability insights.

## Architecture Overview

This project implements a 4-layer dbt architecture:

- **Integration Layer (IL)**: Raw data ingestion from CSV sources
- **Cleansing Layer (CL)**: Data cleaning, standardization, and type casting → Schema: `cl`
- **Operational Layer (OL)**: Business-ready models with enriched data → Schema: `ol`
- **Business Layer (BL)**: KPI aggregations and reporting models → Schema: `bl`

**Output Datasets in BigQuery:**
- `dev.cl` - Development cleansing layer
- `dev.ol` - Development operational layer
- `dev.bl` - Development business layer
- `prod.cl` - Production cleansing layer
- `prod.ol` - Production operational layer
- `prod.bl` - Production business layer

## Project Structure

```
mawave_dbt_project/
├── models/
│   ├── cl/              # Cleansing Layer
│   │   ├── cl_ad_metrics.sql
│   │   ├── cl_campaigns.sql
│   │   ├── cl_clients.sql
│   │   ├── cl_employees.sql
│   │   ├── cl_projects.sql
│   │   ├── cl_social_metrics.sql
│   │   └── cl_time_tracking.sql
│   ├── ol/              # Operational Layer
│   │   ├── ol_unified_ad_metrics.sql
│   │   ├── ol_client_projects_with_time.sql
│   │   └── ol_campaign_profitability.sql
│   └── bl/              # Business Layer
│       ├── bl_client_performance_dashboard.sql
│       ├── bl_resource_utilization.sql
│       └── bl_monthly_campaign_cohorts.sql
├── dbt_project.yml
└── profiles.yml
```

## Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.9+** - [Download Python](https://www.python.org/downloads/)
- **Google Cloud Account** - [Sign up for GCP](https://cloud.google.com/)
- **BigQuery Project** - Create a GCP project with BigQuery enabled
- **Git** - [Install Git](https://git-scm.com/downloads)

---

## Setup Instructions

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd mawave_dbt_project
```

### Step 2: Create a Python Virtual Environment

**For macOS/Linux:**
```bash
python3 -m venv dbt_venv
source dbt_venv/bin/activate
```

**For Windows:**
```bash
python -m venv dbt_venv
dbt_venv\Scripts\activate
```

You should see `(dbt_venv)` appear in your terminal prompt.

### Step 3: Install dbt with BigQuery Adapter

```bash
pip install --upgrade pip
pip install dbt-bigquery
```

Verify installation:
```bash
dbt --version
```

Expected output:
```
Core:
  - installed: 1.8.x
  - latest:    1.8.x - Up to date!

Plugins:
  - bigquery: 1.8.x - Up to date!
```

### Step 4: Set Up Google Cloud Authentication In BQuery

#### Create a Service Account:

1. **Create a Service Account:**
   - Go to [GCP Console → IAM & Admin → Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts)
   - Click "Create Service Account"
   - Name: `dbt-service-account`
   - Grant roles:
     - `BigQuery Data Editor`
     - `BigQuery Job User`
     - `BigQuery User`

2. **Download JSON Key:**
   - Click on the service account
   - Go to "Keys" tab → "Add Key" → "Create New Key"
   - Choose JSON format
   - Save the file in a secure location (e.g., `~/credentials/mawave-service-account.json`)

3. **Set Environment Variable:**

   **For macOS/Linux (add to ~/.bashrc or ~/.zshrc for persistence):**
   ```bash
   export BQ_SERVICE_ACCOUNT="/path/to/mawave-service-account.json"
   ```
   
   **For Windows (PowerShell):**
   ```powershell
   $env:BQ_SERVICE_ACCOUNT="C:\path\to\mawave-service-account.json"
   ```
   
   **For Windows (Command Prompt):**
   ```cmd
   set BQ_SERVICE_ACCOUNT=C:\path\to\mawave-service-account.json
   ```

4. **Verify the environment variable is set:**
   ```bash
   # macOS/Linux
   echo $BQ_SERVICE_ACCOUNT
   
   # Windows PowerShell
   echo $env:BQ_SERVICE_ACCOUNT
   ```

**Security Note:** Never commit the service account JSON file to Git. Add it to `.gitignore`.

### Step 5: Test Your Setup

Run dbt debug to verify everything is configured correctly:

```bash
dbt debug
```

Expected output should show all checks passing:
```
Configuration:
  profiles.yml file [OK found and valid]
  dbt_project.yml file [OK found and valid]

Required dependencies:
  - git [OK found]

Connection:
  method: service-account
  database: mawave-project
  schema: dev
  location: EU
  Connection test: [OK connection ok]

All checks passed!
```

### Step 6: Run Your First dbt Command

Install any dependencies:
```bash
dbt deps
```

Run the models:
```bash
# Run all models
dbt run

# Run specific layer
dbt run --select cl.*      # Cleansing layer
dbt run --select ol.*      # Operational layer
dbt run --select bl.*      # Business layer

# Run specific model
dbt run --select cl_campaigns
```

Run tests:
```bash
dbt test
```

Generate documentation:
```bash
dbt docs generate
dbt docs serve
```

---

## Common dbt Commands

```bash
# Run all models
dbt run

# Run models with tags
dbt run --select tag:cl
dbt run --select tag:ol
dbt run --select tag:bl

# Run a model and all downstream dependencies
dbt run --select cl_campaigns+

# Run a model and all upstream dependencies
dbt run --select +bl_client_performance_dashboard

# Run models that changed
dbt run --select state:modified+

# Compile SQL without running
dbt compile

# Test data quality
dbt test

# Test specific model
dbt test --select cl_campaigns

# Fresh check for source data
dbt source freshness

# Clean project
dbt clean

# Run in production
dbt run --target prod

# Override project variables
dbt run --vars '{"start_date": "2024-06-01"}'
```

## Data Quality & Testing

This project enforces data quality through required tests:

### Required Tests Per Layer:

**All Layers (CL, OL, BL):**
- At least 1 uniqueness test per model
- At least 1 not-null test per model

### Running Tests:

```bash
# Run all tests
dbt test

# Test specific layer
dbt test --select tag:cl
dbt test --select tag:ol
dbt test --select tag:bl

# Test specific model
dbt test --select cl_campaigns

# Run models and tests together
dbt build
```

## Documentation

### Generate Documentation:

```bash
# Generate documentation site
dbt docs generate

# Serve documentation locally
dbt docs serve
```

This will open a browser window with:
- Project lineage graph
- Model documentation
- Column-level descriptions
- Source data information

## Development Workflow

1. **Make changes** to your SQL models
2. **Compile** to verify syntax: `dbt compile --select model_name`
3. **Run** the model: `dbt run --select model_name`
4. **Test** the model: `dbt test --select model_name`
5. **Document** changes in schema YAML files
6. **Commit** to Git

---

## Troubleshooting

### Issue: "Could not find profile named..."

**Solution:** Ensure `profile` in `dbt_project.yml` matches the profile name in `~/.dbt/profiles.yml`

### Issue: "Credentials not found"

**Solution:** 
- Verify `BQ_SERVICE_ACCOUNT` environment variable is set correctly
- Run `echo $BQ_SERVICE_ACCOUNT` (Linux/Mac) or `echo %BQ_SERVICE_ACCOUNT%` (Windows) to check
- Ensure the path points to a valid service account JSON file
- Try restarting your terminal after setting the environment variable

### Issue: "Permission denied" errors

**Solution:** Ensure your service account has these roles:
- `BigQuery Data Editor`
- `BigQuery Job User`
- `BigQuery User`

### Issue: "No connection could be made"

**Solution:**
- Check your internet connection
- Verify GCP project ID is correct
- Ensure BigQuery API is enabled

### Issue: Models fail with "Table not found"

**Solution:**
- Verify source data is uploaded to BigQuery
- Check source configuration in `models/sources.yml`
- Run `dbt run --select cl.*` before `ol.*` or `bl.*`

---

## Project Deliverables

This project includes:

1. **13 dbt Models** across 4 layers (IL → CL → OL → BL)
2. **Data Quality Assessment Report** documenting data issues
3. **Stakeholder Management Document** defining business requirements
4. **dbt Tests** for data quality validation
5. **Documentation** via dbt docs

---

## Key Business Layer Models

### bl_client_performance_dashboard
Client-level profitability overview combining campaign revenue, ad spend, and internal labor costs.

**Key Metrics:**
- Total campaigns, active campaigns
- Total ad spend and revenue
- Net profit and profit margins
- Internal costs and project budgets

### bl_resource_utilization
Employee allocation analysis showing time spent per client and productivity metrics.

**Key Metrics:**
- Total hours per client
- Client time allocation percentage
- Average hourly rate
- Total internal cost

### bl_monthly_campaign_cohorts
Monthly cohort analysis with MoM growth tracking and performance classification.

**Key Metrics:**
- Monthly campaign counts
- ROAS, CTR, CVR averages
- Revenue and spend growth rates
- Profitability status classification
