# 📊 Forex Data Pipeline with dbt

Automated ETL pipeline for downloading forex market data from Twelve Data API to Google Cloud Storage and processing with dbt + BigQuery.

## 🏗️ Project Architecture

```
forex_data_pipeline/
├── models/                  # dbt SQL models
│   ├── staging/            # Staging models (stg_eurusd.sql)
│   └── marts/              # Mart models (fct_eurusd_timeframes.sql)
├── scripts/                # Python scripts
│   └── extract_forex.py   # Data extraction script from API
├── .github/
│   └── workflows/
│       └── daily_run.yml  # GitHub Actions automation
├── requirements.txt       # Python dependencies
├── dbt_project.yml       # dbt configuration
└── README.md
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
# Create virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1  # Windows PowerShell

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Create `.env` file in the root directory:

```env
# Twelve Data API
TWELVE_DATA_API_KEY=your_api_key_here

# Google Cloud Platform
GCS_BUCKET_NAME=your_bucket_name
GCP_PROJECT_ID=your_project_id

# Locations
BIGQUERY_LOCATION=europe-central2
GCS_LOCATION=europe-central2
```

### 3. Local Execution

#### Download data manually

```bash
# Download data for yesterday
python scripts/extract_forex.py --symbol "EUR/USD" --interval "5min"

# Download data for specific date
python scripts/extract_forex.py --date "2026-01-31" --symbol "EUR/USD"
```

#### Run dbt models

```bash
# Run all models
dbt run

# Run staging only
dbt run --select staging.*

# Run marts only
dbt run --select marts.*

# Run tests
dbt test
```

## 🤖 GitHub Actions Automation

Pipeline runs automatically **daily at 1:00 AM UTC** via GitHub Actions.

### Setup Secrets

In GitHub repository settings (Settings → Secrets and variables → Actions) add:

#### Required Secrets:

```
TWELVE_DATA_API_KEY       # API key from Twelve Data
GCS_BUCKET_NAME           # GCS bucket name
GCP_SA_KEY                # Service Account JSON key (full content)
GCP_PROJECT_ID            # GCP project ID
GCP_PRIVATE_KEY_ID        # Private Key ID from Service Account
GCP_PRIVATE_KEY           # Private Key from Service Account
GCP_CLIENT_EMAIL          # Service Account email
GCP_CLIENT_ID             # Service Account Client ID
```

### Manual Trigger

Run workflow manually:
1. Go to **Actions** → **Daily Forex Data Pipeline**
2. Click **Run workflow**
3. (Optional) Specify parameters: date, symbol, interval

## 📈 Pipeline Workflow

### 1. Extract (Python)
- `scripts/extract_forex.py` downloads data from Twelve Data API
- Implements retry logic with exponential backoff for rate limits
- Stores data in GCS as partitioned Parquet files

### 2. Transform & Load (dbt)
- **Staging**: `stg_eurusd.sql` - loads raw data from GCS
- **Marts**: `fct_eurusd_timeframes.sql` - aggregates data by timeframe

### 3. Data Partitioning

```
gs://your-bucket/
└── eur_usd/
    └── year=2026/
        └── month=01/
            ├── data_2026_01_30.parquet
            └── data_2026_01_31.parquet
```

## 🔧 Configuration

### dbt Profile

Create `~/.dbt/profiles.yml`:

```yaml
forex_data_pipeline:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-project-id
      dataset: forex_data
      location: EU
      keyfile: /path/to/keyfile.json
      threads: 4
```

### GitHub Actions Workflow

File [.github/workflows/daily_run.yml](.github/workflows/daily_run.yml) contains:
- **Schedule trigger**: Daily run at 1 AM UTC
- **Manual trigger**: For testing with parameters
- **Two jobs**: 
  1. `extract-and-load` - data extraction
  2. `run-dbt` - dbt model execution

## 📊 dbt Models

### Staging: `stg_eurusd.sql`
Loads raw data from GCS external table

### Marts: `fct_eurusd_timeframes.sql`
Creates aggregated fact tables for different timeframes

## 🔐 Security

- All credentials stored in GitHub Secrets
- Local `.json` keys excluded via `.gitignore`
- Service Account has minimal required permissions

## 📝 Logging

GitHub Actions provides comprehensive logs for each run:
- Number of records fetched
- GCS upload status
- dbt model execution results
- Errors and warnings

## 🛠️ Development

### Adding New Currency Pair

1. Update workflow for new symbol
2. Create corresponding dbt models
3. Update `dbt_project.yml`

### Changing Interval

Supported intervals: `1min`, `5min`, `15min`, `30min`, `1h`, `1day`

## 🐛 Troubleshooting

### Rate Limits
Script automatically handles rate limits with exponential backoff

### GCS Authentication
Ensure Service Account has permissions:
- `storage.objects.create`
- `storage.objects.get`
- `bigquery.tables.create`
- `bigquery.tables.updateData`

## 📚 Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [Twelve Data API](https://twelvedata.com/docs)
- [GitHub Actions](https://docs.github.com/en/actions)
- [Google Cloud Storage](https://cloud.google.com/storage/docs)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)

## 📄 License

MIT
