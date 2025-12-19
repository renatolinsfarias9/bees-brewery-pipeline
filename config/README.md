# Google Cloud Storage (GCS) Configuration

This directory contains Google Cloud Platform credentials for cloud storage integration.

## Overview

This project implements a **dual-storage architecture** following industry best practices:
- **Local Storage**: For development and testing
- **Cloud Storage (GCS)**: For production deployment with enhanced capabilities

## Features Enabled by GCS

When GCS is configured, the pipeline gains these capabilities:

### 1. **Data Partitioning** (Silver Layer)
- Partitions data by `country` and `state_province`
- Optimizes query performance for location-based analytics
- Reduces data scanning for targeted queries

### 2. **Dual Persistence**
- All data is saved to BOTH local and GCS
- Local: Fast access for development
- GCS: Scalable, durable cloud storage

### 3. **Data Priority**
- Reads from local storage first
- If GCS is available, reads from GCS (overwrites local)
- Ensures cloud data is the source of truth

## Setup Instructions

### 1. Create Google Cloud Platform Account
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (e.g., `brewery-data-pipeline`)

### 2. Enable Required APIs
```bash
# Enable Cloud Storage API
gcloud services enable storage-api.googleapis.com
```

### 3. Create Service Account
```bash
# Create service account
gcloud iam service-accounts create brewery-pipeline \
    --display-name="Brewery Data Pipeline"

# Grant storage permissions
gcloud projects add-iam-policy-binding PROJECT_ID \
    --member="serviceAccount:brewery-pipeline@PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"
```

### 4. Download Credentials
```bash
# Download JSON key
gcloud iam service-accounts keys create gcs_config.json \
    --iam-account=brewery-pipeline@PROJECT_ID.iam.gserviceaccount.com
```

### 5. Place Credentials File
Move the downloaded `gcs_config.json` to this directory:
```bash
cp ~/Downloads/gcs_config.json ./config/gcs_config.json
```

### 6. Create GCS Bucket
```bash
# Create bucket (choose a globally unique name)
gsutil mb -p PROJECT_ID -l US gs://brewery-data-pipeline/

# Verify bucket creation
gsutil ls
```

### 7. Configure Environment
Edit `.env` file and set:
```bash
GCS_BUCKET_NAME=brewery-data-pipeline
```

### 8. Restart Docker Containers
```bash
docker compose down
docker compose up --build -d
```

## Verification

### Check GCS Integration Status
Look for these log messages in Airflow:

**When GCS is enabled:**
```
GCS storage client initialized successfully
GCS enabled - will write WITH partitioning to gs://brewery-data-pipeline
Successfully wrote partitioned data to GCS
```

**When GCS is NOT available:**
```
GCS credentials not found. Running in local-only mode.
GCS not available - will save locally only
```

### Verify Data in GCS
```bash
# List bronze layer
gsutil ls gs://brewery-data-pipeline/bronze/

# List silver layer (partitioned)
gsutil ls -r gs://brewery-data-pipeline/silver/

# List gold layer
gsutil ls -r gs://brewery-data-pipeline/gold/
```

## Architecture

### Data Flow with GCS

```
┌─────────────────┐
│   API Source    │
└────────┬────────┘
         │
         v
    ┌────────┐
    │ Bronze │ ──> Saves to: Local + GCS
    └────────┘
         │
         v (reads from GCS if available)
    ┌────────┐
    │ Silver │ ──> Saves to: Local (no partition) + GCS (with partition)
    └────────┘
         │
         v (reads from GCS if available)
    ┌────────┐
    │  Gold  │ ──> Saves to: Local + GCS
    └────────┘
```

### Storage Comparison

| Feature | Local Storage | GCS Storage |
|---------|--------------|-------------|
| **Setup** | Simple (included) | Requires credentials |
| **Cost** | Free | Pay per GB |
| **Partitioning** | Limited (retries cause conflicts) | Full support |
| **Scalability** | Limited by disk | Unlimited |
| **Durability** | Single machine | 99.999999999% durability |
| **Access** | Local only | Global |

## Troubleshooting

### Error: "GCS credentials not found"
**Solution**: Ensure `gcs_config.json` exists in `./config/` directory

### Error: "Permission denied" when writing to GCS
**Solution**: Check service account has `roles/storage.admin` permission

### Error: "Bucket not found"
**Solution**:
1. Verify bucket exists: `gsutil ls`
2. Check bucket name in `.env` matches actual bucket

### Data not appearing in GCS
**Solution**:
1. Check Airflow logs for errors
2. Verify `GCS_BUCKET_NAME` is set in `.env`
3. Ensure credentials are valid

## Cost Optimization

### Free Tier Limits
GCS offers these free tier limits monthly:
- 5 GB storage
- 5,000 Class A operations
- 50,000 Class B operations
- 1 GB network egress (Americas)

### Cost Estimation
For this pipeline (daily runs):
- Storage: ~10 MB/day = ~300 MB/month ✅ Free
- Operations: ~100/day = ~3,000/month ✅ Free
- **Total monthly cost**: ~$0.00 (within free tier)

## Security Best Practices

1. **Never commit `gcs_config.json` to git**
   - Already in `.gitignore`
   - Contains sensitive credentials

2. **Rotate credentials regularly**
   ```bash
   # Create new key
   gcloud iam service-accounts keys create new-key.json \
       --iam-account=brewery-pipeline@PROJECT_ID.iam.gserviceaccount.com

   # Delete old key
   gcloud iam service-accounts keys delete KEY_ID \
       --iam-account=brewery-pipeline@PROJECT_ID.iam.gserviceaccount.com
   ```

3. **Use least-privilege permissions**
   - Service account only has storage access
   - No compute or other permissions

## Running Without GCS

The pipeline works perfectly without GCS:
1. Leave `GCS_BUCKET_NAME` empty in `.env`
2. Don't create `gcs_config.json`
3. Pipeline will run in local-only mode

**Trade-offs:**
- ✅ Simpler setup
- ✅ No cloud costs
- ❌ No partitioning by location (silver layer)
- ❌ Limited scalability

## Reference Project

This implementation follows the architecture of a successful candidate project,
adapted with improvements:
- ✅ Graceful fallback when GCS unavailable
- ✅ Clear logging of GCS status
- ✅ Comprehensive documentation
- ✅ Security best practices

## Support

For issues with GCS integration:
1. Check Airflow logs: `docker compose logs airflow-scheduler`
2. Verify credentials: `gcloud auth activate-service-account --key-file=config/gcs_config.json`
3. Test bucket access: `gsutil ls gs://YOUR_BUCKET_NAME/`
