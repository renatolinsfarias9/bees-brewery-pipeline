"""
Brewery Data Pipeline DAG - Medallion Architecture Implementation.

This DAG orchestrates the end-to-end brewery data pipeline with three layers:
- Bronze: Raw data ingestion from Open Brewery DB API
- Silver: Data transformation and partitioning by location
- Gold: Aggregated analytical views

Schedule: Daily at 2 AM UTC
Retries: 3 attempts with 5-minute delay
SLA: 2 hours
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Import pipeline modules
import sys

sys.path.insert(0, "/opt/airflow/src")

from bronze import ingestion as bronze_ingestion
from silver import transformation as silver_transformation
from gold import aggregation as gold_aggregation
from utils import config as utils_config
from utils import logger as utils_logger

# Create aliases for easier use
BreweryAPIClient = bronze_ingestion.BreweryAPIClient
ingest_breweries = bronze_ingestion.ingest_breweries
transform_to_silver = silver_transformation.transform_to_silver
create_gold_aggregations = gold_aggregation.create_gold_aggregations
get_config = utils_config.get_config
setup_logger = utils_logger.setup_logger

logger = setup_logger(__name__)

# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-eng@bees.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "sla": timedelta(hours=2),
}


def bronze_ingestion_task(**context):
    """Ingest raw brewery data from API to bronze layer."""
    logger.info("Starting bronze layer ingestion")

    # Get configuration
    config = get_config()
    api_client = BreweryAPIClient(config.api)

    # Get execution date from Airflow context
    execution_date = context["ds"]  # Format: YYYY-MM-DD

    # Perform ingestion
    output_file = ingest_breweries(
        api_client=api_client,
        output_path=config.datalake.bronze_path,
        execution_date=execution_date,
        gcs_bucket=config.datalake.gcs_bucket,
    )

    logger.info(f"Bronze ingestion completed. Output: {output_file}")

    # Push metadata to XCom for downstream tasks
    context["ti"].xcom_push(key="bronze_output", value=output_file)

    return output_file


def silver_transformation_task(**context):
    """Transform bronze data to silver layer with partitioning."""
    logger.info("Starting silver layer transformation")

    # Get configuration
    config = get_config()

    # Get execution date
    execution_date = context["ds"]

    # Perform transformation
    output_path = transform_to_silver(
        bronze_path=config.datalake.bronze_path,
        silver_path=config.datalake.silver_path,
        execution_date=execution_date,
        gcs_bucket=config.datalake.gcs_bucket,
    )

    logger.info(f"Silver transformation completed. Output: {output_path}")

    # Push metadata to XCom
    context["ti"].xcom_push(key="silver_output", value=output_path)

    return output_path


def gold_aggregation_task(**context):
    """Create gold layer aggregations from silver data."""
    logger.info("Starting gold layer aggregation")

    # Get configuration
    config = get_config()

    # Get execution date
    execution_date = context["ds"]

    # Perform aggregation
    output_path = create_gold_aggregations(
        silver_path=config.datalake.silver_path,
        gold_path=config.datalake.gold_path,
        execution_date=execution_date,
        gcs_bucket=config.datalake.gcs_bucket,
    )

    logger.info(f"Gold aggregation completed. Output: {output_path}")

    # Push metadata to XCom
    context["ti"].xcom_push(key="gold_output", value=output_path)

    return output_path


def data_quality_check(**context):
    """
    Perform basic data quality checks across all layers.

    Args:
        **context: Airflow context
    """
    logger.info("Performing data quality checks")

    config = get_config()
    execution_date = context["ds"]

    # Check if all layer outputs exist
    bronze_path = Path(config.datalake.bronze_path) / f"date={execution_date}"
    silver_path = Path(config.datalake.silver_path) / f"date={execution_date}"
    gold_path = Path(config.datalake.gold_path) / f"date={execution_date}"

    checks_passed = []
    checks_failed = []

    # Bronze layer check
    if bronze_path.exists() and list(bronze_path.glob("*.json")):
        checks_passed.append("Bronze layer data exists")
    else:
        checks_failed.append("Bronze layer data missing")

    # Silver layer check
    if silver_path.exists() and list(silver_path.rglob("*.parquet")):
        checks_passed.append("Silver layer data exists")
    else:
        checks_failed.append("Silver layer data missing")

    # Gold layer check
    if gold_path.exists() and list(gold_path.rglob("*.parquet")):
        checks_passed.append("Gold layer data exists")
    else:
        checks_failed.append("Gold layer data missing")

    # Log results
    logger.info(f"Quality checks passed: {checks_passed}")

    if checks_failed:
        logger.error(f"Quality checks failed: {checks_failed}")
        raise ValueError(f"Data quality checks failed: {checks_failed}")

    logger.info("All data quality checks passed")

    return {"passed": checks_passed, "failed": checks_failed}


# Define the DAG
with DAG(
    dag_id="brewery_data_pipeline",
    default_args=default_args,
    description="End-to-end brewery data pipeline with medallion architecture",
    schedule_interval="0 2 * * *",  # Daily at 2 AM UTC
    start_date=datetime(2025, 12, 14),
    catchup=False,
    tags=["brewery", "medallion", "etl", "bees"],
    max_active_runs=1,
) as dag:

    # Start marker
    start = EmptyOperator(task_id="start")

    # Bronze layer ingestion
    with TaskGroup("bronze_layer", tooltip="Raw data ingestion") as bronze_group:
        bronze_ingest = PythonOperator(
            task_id="ingest_from_api",
            python_callable=bronze_ingestion_task,
            provide_context=True,
        )

    # Silver layer transformation
    with TaskGroup("silver_layer", tooltip="Data transformation and partitioning") as silver_group:
        silver_transform = PythonOperator(
            task_id="transform_and_partition",
            python_callable=silver_transformation_task,
            provide_context=True,
        )

    # Gold layer aggregation
    with TaskGroup("gold_layer", tooltip="Analytical aggregations") as gold_group:
        gold_aggregate = PythonOperator(
            task_id="create_aggregations",
            python_callable=gold_aggregation_task,
            provide_context=True,
        )

    # Data quality checks
    quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
        provide_context=True,
    )

    # End marker
    end = EmptyOperator(task_id="end")

    # Define task dependencies
    start >> bronze_group >> silver_group >> gold_group >> quality_check >> end
