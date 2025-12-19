# BEES Data Engineering - Breweries Case

This repository contains the implementation of the BEES Data Engineering technical challenge.

The project was developed and tested on **Windows 11** with **Python 3.11** and **Apache Airflow 2.8.0**.

---

## Project Structure

| Item | Description |
|------|-------------|
| `config/` | Google Cloud Storage configuration files (protected) |
| `dags/` | Airflow DAGs for orchestrating the data pipeline |
| `src/` | Source code for the ETL pipeline (Bronze, Silver, Gold layers) |
| `tests/` | Unit and integration tests |
| `data/` | Local data lake storage (gitignored) |
| `logs/` | Airflow execution logs (gitignored) |
| `.env.example` | Template for environment variables |
| `docker-compose.yml` | Docker Compose configuration for local execution |
| `Dockerfile` | Custom Docker image with Airflow + PySpark |
| `requirements.txt` | Python dependencies |
| `pytest.ini` | Test configuration |
| `Makefile` | Utility commands for setup and execution |

---

## Study Case Specifications

**Objective:**
Consume data from the Open Brewery DB API, transform it, and persist it into a data lake following the **medallion architecture** with three layers:

- **Bronze Layer:** Raw data ingestion in JSON format
- **Silver Layer:** Curated data transformed to Parquet and partitioned by location
- **Gold Layer:** Analytical aggregations ready for business consumption

**API Endpoint:**
https://api.openbrewerydb.org/v1/breweries

**Orchestration:**
Apache Airflow 2.8.0 is used for orchestration with:
- Automatic retries (3 attempts with 5-minute delay)
- SLA monitoring (2 hours)
- Email alerts on failures

**Airflow Credentials:**
- **Username:** `airflow`
- **Password:** `airflow`

**Language:**
Python 3.11 with PySpark 3.5.0 for distributed data processing.

**Containerization:**
Docker Compose is used for containerization. The custom image includes:
- Apache Airflow 2.8.0
- PySpark 3.5.0 with Java 17
- All required Python dependencies

**Data Lake Architecture:**

| Layer | Format | Description |
|-------|--------|-------------|
| **Bronze** | JSON | Raw data from API, partitioned by execution date |
| **Silver** | Parquet | Cleaned and transformed data, partitioned by country/state |
| **Gold** | Parquet | Aggregated analytical views (3 datasets) |

**Monitoring:**
Airflow Webserver UI is used to monitor:
- DAG execution status
- Task-level logs and errors
- Execution history and metrics

**Repository:**
GitHub is used for version control.

**Cloud Services:**
The project integrates with **Google Cloud Storage** for cloud data persistence.
To enable GCS integration, request the configuration file from the project owner and place it at:
- `config/gcs_config.json`

**Note:** The pipeline works without GCS (local-only mode) for evaluation purposes.

---

## Quick Start

### Prerequisites

- Docker Desktop (20.10+)
- Docker Compose (2.0+)
- 8GB RAM minimum
- 10GB available disk space

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/bees-brewery-pipeline.git
cd bees-brewery-pipeline
```

### 2. Configure Environment

```bash
cp .env.example .env
```

### 3. Start the Pipeline

```bash
docker-compose up -d
```

Wait ~30 seconds for initialization.

### 4. Access Airflow UI

- **URL:** http://localhost:8080
- **Username:** `airflow`
- **Password:** `airflow`

### 5. Run the Pipeline

1. In the Airflow UI, locate the DAG `brewery_data_pipeline`
2. Click the **Play button** → "Trigger DAG"
3. Monitor execution in the **Graph View** or **Grid View**

**Expected Duration:** 6-15 minutes for processing ~9,000 breweries

---

## DAG: brewery_data_pipeline

This pipeline extracts, transforms, and loads brewery data following the medallion architecture.

### Pipeline Flow

```
start → bronze_layer → silver_layer → gold_layer → quality_checks → end
```

### Task Details

| Task | Description | Duration |
|------|-------------|----------|
| `bronze_layer.ingest_from_api` | Fetch data from API with pagination and retry logic | 2-5 min |
| `silver_layer.transform_and_partition` | Transform to Parquet with PySpark, partition by location | 3-7 min |
| `gold_layer.create_aggregations` | Create 3 analytical views | 1-2 min |
| `data_quality_check` | Validate data presence in all layers | 10 sec |

### Schedule

- **Frequency:** Daily at 2 AM UTC
- **Catchup:** Disabled
- **Max Active Runs:** 1

---

## Testing

### Run All Tests

```bash
docker-compose exec airflow-webserver pytest -v
```

### Run with Coverage

```bash
docker-compose exec airflow-webserver pytest --cov=src --cov-report=term-missing
```

### Test Structure

- `tests/unit/` - Unit tests with mocked dependencies
- `tests/integration/` - End-to-end integration tests

---

## Data Lake Structure

```
data/
├── bronze/
│   └── date=YYYY-MM-DD/
│       ├── breweries_*.json          # Raw API data (~4.6 MB)
│       └── metadata_*.json           # Execution metadata
├── silver/
│   └── date=YYYY-MM-DD/
│       └── breweries_per_location/   # Parquet files (~1.5 MB)
└── gold/
    └── date=YYYY-MM-DD/
        ├── brewery_by_type/          # Global aggregation by type
        ├── brewery_by_location/      # Aggregation by location
        └── brewery_by_type_and_location/  # Detailed aggregation
```

<img width="538" height="972" alt="image" src="https://github.com/user-attachments/assets/ddeb396d-5cf6-45cd-b264-32a0030eb1b5" />


---

## Cloud Storage (Optional)

To enable Google Cloud Storage integration:

1. **Request Configuration:** Contact the project owner for `gcs_config.json`
2. **Place File:** Save at `config/gcs_config.json`
3. **Set Bucket Name:** Add to `.env` file:
   ```
   GCS_BUCKET_NAME=your-bucket-name
   ```
4. **Restart Services:**
   ```bash
   docker-compose restart
   ```

**Note:** The pipeline works in local-only mode if GCS is not configured.

---

## Required Files (Not Included)

For security reasons, the following files are **not** included in the repository:

| File | Description |
|------|-------------|
| `config/gcs_config.json` | Google Cloud Storage service account credentials |
| `.env` | Environment variables (use `.env.example` as template) |

---

## Useful Commands

```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f airflow-scheduler

# Run tests
docker-compose exec airflow-webserver pytest -v

# Clean all data
rm -rf data/bronze data/silver data/gold

# Restart services
docker-compose restart
```

---

## Troubleshooting

### Airflow UI not accessible

```bash
docker-compose ps                      # Check container status
docker-compose logs airflow-webserver  # Check logs
docker-compose restart airflow-webserver
```

### Permission errors in data/

```bash
chmod -R 777 data/
```

### Pipeline fails on retry

```bash
# Clear failed task state in Airflow UI
# Task → Clear → Confirm
```

---

## Architecture Highlights

- ✅ **Medallion Architecture:** Bronze → Silver → Gold layers
- ✅ **Distributed Processing:** PySpark for scalable transformations
- ✅ **Cloud Integration:** Google Cloud Storage with partitioned data
- ✅ **Retry Logic:** Exponential backoff for API failures
- ✅ **Data Quality:** Automated validation checks
- ✅ **Containerized:** Fully dockerized for reproducibility
- ✅ **Tested:** Unit and integration tests included

---

## Technology Stack

- **Orchestration:** Apache Airflow 2.8.0
- **Processing:** PySpark 3.5.0
- **Storage:** Parquet (Snappy compression)
- **Cloud:** Google Cloud Storage
- **Containerization:** Docker + Docker Compose
- **Testing:** Pytest
- **Language:** Python 3.11

---

## Contact

For access to protected configuration files (`gcs_config.json`, `.env`), please contact the project owner.

---

**Status:** ✅ Production Ready | 🐳 Containerized | ☁️ Cloud Integrated | 🧪 Tested
