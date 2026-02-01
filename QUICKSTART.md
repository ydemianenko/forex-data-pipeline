# 🚀 Quick Start Forex Data Pipeline

## For New Users

### 1. Clone the Repository (if using GitHub)
```bash
git clone https://github.com/your-username/forex-data-pipeline.git
cd forex-data-pipeline
```

### 2. Create Virtual Environment
```powershell
# Windows PowerShell
python -m venv .venv
.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure Environment Variables
```powershell
# Copy template
Copy-Item .env.example .env

# Edit .env file and add your keys:
# - TWELVE_DATA_API_KEY
# - GCS_BUCKET_NAME
# - GCP_PROJECT_ID
# - GOOGLE_APPLICATION_CREDENTIALS
```

### 4. Verify Setup
```powershell
python scripts/test_setup.py
```

If all checks pass ✅ - you're ready!

---

## Local Execution

### Download data for yesterday
```powershell
python scripts/extract_forex.py
```

### Download data for specific date
```powershell
python scripts/extract_forex.py --date "2026-01-31"
```

### Download other currency pair
```powershell
python scripts/extract_forex.py --symbol "GBP/USD" --interval "1h"
```

### Run dbt models
```powershell
# Verify connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test
```

---

## GitHub Actions Setup

### 1. Push code to GitHub
```bash
git add .
git commit -m "Setup forex data pipeline"
git push origin main
```

### 2. Add Secrets
Go to **Settings** → **Secrets and variables** → **Actions**

Add the following secrets (see [SETUP_SECRETS.md](SETUP_SECRETS.md) for details):
- `TWELVE_DATA_API_KEY`
- `GCS_BUCKET_NAME`
- `GCP_SA_KEY`
- `GCP_PROJECT_ID`
- `GCP_PRIVATE_KEY_ID`
- `GCP_PRIVATE_KEY`
- `GCP_CLIENT_EMAIL`
- `GCP_CLIENT_ID`

### 3. Run workflow manually
1. Go to **Actions** tab
2. Select **Daily Forex Data Pipeline**
3. Click **Run workflow**
4. Run with parameters or use defaults

### 4. Automatic Execution
After setup, the workflow will run automatically **daily at 1:00 AM UTC**

---

## Data Structure in GCS

```
gs://your-bucket/
└── eur_usd/
    └── year=2026/
        └── month=01/
            └── data_2026_01_31.parquet
```

---

## Data Structure in BigQuery

### Dataset: `forex_data_staging`
- `stg_eurusd` - raw data from GCS

### Dataset: `forex_data_marts`
- `fct_eurusd_timeframes` - aggregated data

---

## Useful Commands

### Check GCS status
```powershell
gsutil ls gs://your-bucket/eur_usd/
```

### Check BigQuery tables
```powershell
bq ls forex_data_staging
bq ls forex_data_marts
```

### View data
```powershell
bq head -n 10 forex_data_marts.fct_eurusd_timeframes
```

### Monitor GitHub Actions
```
https://github.com/your-username/forex-data-pipeline/actions
```

---

## Troubleshooting

### "Permission denied" when running script
```powershell
# Make sure credentials are set
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\key.json"
```

### "Rate limit exceeded" from Twelve Data
- Reduce request frequency
- Upgrade to paid plan
- Increase interval between requests

### "Bucket not found"
```powershell
# Create bucket
gsutil mb -p your-project-id -l US gs://your-bucket-name/
```

### dbt cannot connect to BigQuery
```powershell
# Check profile
dbt debug

# Make sure ~/.dbt/profiles.yml is configured correctly
```

---

## Additional Resources

- 📖 [Full README](README.md)
- 🔐 [Secrets Setup](SETUP_SECRETS.md)
- 🐛 [Test Setup](scripts/test_setup.py)
- 📊 [dbt Documentation](https://docs.getdbt.com/)

---

## Support

If you have questions:
1. Check GitHub Actions logs
2. Run `python scripts/test_setup.py`
3. Review documentation above

**Good luck with forex data analysis! 📈**
