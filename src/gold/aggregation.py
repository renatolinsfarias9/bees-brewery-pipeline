"""
Gold Layer - Aggregated analytical views.

Creates business-ready aggregations from silver layer data.
"""

import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.logger import setup_logger
from utils.gcs_storage import GcsStorage

logger = setup_logger(__name__)


def get_spark_session(app_name: str = "BreweryPipeline-Gold") -> SparkSession:
    """Create or get existing Spark session."""
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.validateOutputSpecs", "false")
        .getOrCreate()
    )

    return spark


def aggregate_by_type_and_location(df: DataFrame) -> DataFrame:
    """Aggregate breweries by type and location with quality metrics."""
    logger.info("Creating aggregation by type and location")

    # Main aggregation
    agg_df = (
        df.groupBy(
            "country_normalized",
            "state_province_normalized",
            "brewery_type_normalized",
        )
        .agg(
            F.count("*").alias("brewery_count"),
            F.sum(F.when(F.col("has_coordinates"), 1).otherwise(0)).alias(
                "with_coordinates"
            ),
            F.sum(F.when(F.col("has_contact_info"), 1).otherwise(0)).alias(
                "with_contact_info"
            ),
            F.countDistinct("id").alias("unique_breweries"),
        )
        .withColumnRenamed("country_normalized", "country")
        .withColumnRenamed("state_province_normalized", "state_province")
        .withColumnRenamed("brewery_type_normalized", "brewery_type")
    )

    # Add percentage calculations
    window_spec = Window.partitionBy("country", "state_province")

    agg_df = agg_df.withColumn(
        "percentage_of_location",
        F.round((F.col("brewery_count") / F.sum("brewery_count").over(window_spec)) * 100, 2),
    )

    # Add data quality score (percentage with complete info)
    agg_df = agg_df.withColumn(
        "data_quality_score",
        F.round(
            ((F.col("with_coordinates") + F.col("with_contact_info")) / (F.col("brewery_count") * 2)) * 100,
            2,
        ),
    )

    # Add timestamp
    agg_df = agg_df.withColumn("aggregation_timestamp", F.current_timestamp())

    # Sort for better readability
    agg_df = agg_df.orderBy("country", "state_province", F.desc("brewery_count"))

    logger.info(f"Aggregation completed. {agg_df.count()} aggregate records created")

    return agg_df


def aggregate_by_type(df: DataFrame) -> DataFrame:
    """Aggregate breweries globally by type."""
    logger.info("Creating global aggregation by brewery type")

    agg_df = (
        df.groupBy("brewery_type_normalized")
        .agg(
            F.count("*").alias("total_breweries"),
            F.countDistinct("country_normalized").alias("countries_count"),
            F.countDistinct("state_province_normalized").alias("regions_count"),
            F.avg(
                F.when(F.col("has_coordinates"), 1).otherwise(0)
            ).alias("avg_coordinate_completeness"),
        )
        .withColumnRenamed("brewery_type_normalized", "brewery_type")
    )

    # Calculate percentage of total
    total_breweries = df.count()
    agg_df = agg_df.withColumn(
        "percentage_of_total",
        F.round((F.col("total_breweries") / total_breweries) * 100, 2),
    )

    # Add timestamp
    agg_df = agg_df.withColumn("aggregation_timestamp", F.current_timestamp())

    # Sort by count descending
    agg_df = agg_df.orderBy(F.desc("total_breweries"))

    logger.info(f"Global aggregation completed. {agg_df.count()} brewery types found")

    return agg_df


def aggregate_by_location(df: DataFrame) -> DataFrame:
    """Aggregate breweries by location with type diversity."""
    logger.info("Creating aggregation by location")

    agg_df = (
        df.groupBy("country_normalized", "state_province_normalized")
        .agg(
            F.count("*").alias("total_breweries"),
            F.countDistinct("brewery_type_normalized").alias("brewery_types_count"),
            F.sum(F.when(F.col("has_coordinates"), 1).otherwise(0)).alias(
                "with_coordinates"
            ),
            F.collect_set("brewery_type_normalized").alias("brewery_types"),
        )
        .withColumnRenamed("country_normalized", "country")
        .withColumnRenamed("state_province_normalized", "state_province")
    )

    # Add timestamp
    agg_df = agg_df.withColumn("aggregation_timestamp", F.current_timestamp())

    # Sort by country and brewery count
    agg_df = agg_df.orderBy("country", F.desc("total_breweries"))

    logger.info(f"Location aggregation completed. {agg_df.count()} locations found")

    return agg_df


def create_gold_aggregations(
    silver_path: str,
    gold_path: str,
    execution_date: Optional[str] = None,
    gcs_bucket: Optional[str] = None,
) -> str:
    """Create gold layer aggregations with three analytical views."""
    try:
        exec_date = execution_date or datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Starting gold layer aggregation for date: {exec_date}")

        # Initialize Spark
        spark = get_spark_session()

        # Initialize GCS storage
        gcs_storage = GcsStorage() if gcs_bucket else None

        # === READING PHASE (following reference pattern) ===

        # Read from LOCAL silver layer
        input_path = Path(silver_path) / f"date={exec_date}"

        if not input_path.exists():
            raise FileNotFoundError(f"Silver data not found at {input_path}")

        logger.info(f"Reading silver data from local: {input_path}")
        # Read from partitioned breweries_per_location directory
        df = spark.read.parquet(str(input_path / "breweries_per_location"))
        logger.info(f"Loaded {df.count()} records from local silver layer (partitioned)")

        # Note: GCS reading with Spark requires GCS connector (not configured)
        # For now, we use local Silver data, which is always available
        # GCS is used for uploading aggregation results

        # Create output directory
        output_path = Path(gold_path) / f"date={exec_date}"

        # Clean ALL Spark staging directories in gold layer
        # Spark creates these during write operations and they can cause conflicts on retries
        parent_path = Path(gold_path)
        for spark_staging in parent_path.rglob(".spark-staging-*"):
            logger.info(f"Removing Spark staging directory: {spark_staging}")
            shutil.rmtree(spark_staging, ignore_errors=True)

        # Clean output directory if it exists to avoid conflicts
        if output_path.exists():
            logger.info(f"Removing existing output directory: {output_path}")
            shutil.rmtree(output_path, ignore_errors=True)

        # === AGGREGATION & WRITING PHASE (save to BOTH local and GCS) ===

        # Generate all aggregations
        agg_type_location = aggregate_by_type_and_location(df)
        agg_type = aggregate_by_type(df)
        agg_location = aggregate_by_location(df)

        # 1. Brewery by type and location
        logger.info("Saving brewery_by_type_and_location aggregation...")

        # Save to local
        type_location_path = output_path / "brewery_by_type_and_location"
        agg_type_location.write.mode("overwrite").parquet(str(type_location_path))
        logger.info(f"Saved to local: {type_location_path}")

        # Upload to GCS (if available)
        if gcs_storage and gcs_storage.is_enabled():
            success = gcs_storage.save_parquet_to_gcs(
                local_path=str(type_location_path),
                bucket_name=gcs_bucket,
                layer_name="gold",
                folder_name=f"date={exec_date}/brewery_by_type_and_location"
            )
            if success:
                logger.info(f"Uploaded to GCS: gs://{gcs_bucket}/gold/date={exec_date}/brewery_by_type_and_location")

        # 2. Brewery by type (global)
        logger.info("Saving brewery_by_type aggregation...")

        # Save to local
        type_path = output_path / "brewery_by_type"
        agg_type.write.mode("overwrite").parquet(str(type_path))
        logger.info(f"Saved to local: {type_path}")

        # Upload to GCS (if available)
        if gcs_storage and gcs_storage.is_enabled():
            success = gcs_storage.save_parquet_to_gcs(
                local_path=str(type_path),
                bucket_name=gcs_bucket,
                layer_name="gold",
                folder_name=f"date={exec_date}/brewery_by_type"
            )
            if success:
                logger.info(f"Uploaded to GCS: gs://{gcs_bucket}/gold/date={exec_date}/brewery_by_type")

        # 3. Brewery by location
        logger.info("Saving brewery_by_location aggregation...")

        # Save to local
        location_path = output_path / "brewery_by_location"
        agg_location.write.mode("overwrite").parquet(str(location_path))
        logger.info(f"Saved to local: {location_path}")

        # Upload to GCS (if available)
        if gcs_storage and gcs_storage.is_enabled():
            success = gcs_storage.save_parquet_to_gcs(
                local_path=str(location_path),
                bucket_name=gcs_bucket,
                layer_name="gold",
                folder_name=f"date={exec_date}/brewery_by_location"
            )
            if success:
                logger.info(f"Uploaded to GCS: gs://{gcs_bucket}/gold/date={exec_date}/brewery_by_location")

        # Summary
        summary = f"Successfully created all gold layer aggregations at {output_path}"
        if gcs_storage and gcs_storage.is_enabled():
            summary += f" and gs://{gcs_bucket}/gold/date={exec_date}"
        logger.info(summary)

        # Stop Spark session
        spark.stop()

        return str(output_path)

    except Exception as e:
        logger.error(f"Error during gold aggregation: {str(e)}")
        raise
