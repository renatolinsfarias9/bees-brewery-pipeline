"""
Google Cloud Storage integration module.

This module provides GCS functionality with graceful fallback to local-only mode
if GCS credentials are not available. This allows the pipeline to work both with
and without cloud storage, making it easier to evaluate and demonstrate.
"""

import json
import os
from pathlib import Path
from typing import Optional

from utils.logger import setup_logger

logger = setup_logger(__name__)

# Try to import GCS library, but don't fail if not available
try:
    from google.cloud import storage
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    logger.warning("google-cloud-storage not installed. GCS features will be disabled.")


class GcsStorage:
    """
    A class to interact with Google Cloud Storage buckets.

    Falls back to local-only mode if GCS is not available or not configured.
    This allows the pipeline to work in both cloud and local-only environments.
    """

    def __init__(self, credentials_path: Optional[str] = None):
        """
        Initialize GCS client if credentials are available.

        Args:
            credentials_path: Path to GCS credentials JSON file
        """
        self.enabled = False
        self.storage_client = None

        # Default credentials path
        if credentials_path is None:
            credentials_path = os.getenv(
                "GOOGLE_APPLICATION_CREDENTIALS",
                "/opt/airflow/config/gcs_config.json"
            )

        # Check if GCS is available and configured
        if not GCS_AVAILABLE:
            logger.warning("GCS library not available. Running in local-only mode.")
            return

        if not Path(credentials_path).exists():
            logger.warning(
                f"GCS credentials not found at {credentials_path}. "
                "Running in local-only mode. To enable GCS, provide credentials file."
            )
            return

        try:
            # Set credentials environment variable
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

            # Initialize storage client
            self.storage_client = storage.Client()
            self.enabled = True
            logger.info("GCS storage client initialized successfully")

        except Exception as e:
            logger.warning(
                f"Failed to initialize GCS client: {str(e)}. "
                "Running in local-only mode."
            )

    def is_enabled(self) -> bool:
        """Check if GCS is enabled and available."""
        return self.enabled

    def get_path_in_layer(
        self, bucket_name: str, layer_name: str, file_name: str = ""
    ) -> str:
        """
        Generate a GCS path string for a given layer.

        Args:
            bucket_name: Name of the GCS bucket
            layer_name: Layer name (bronze, silver, gold)
            file_name: Optional file name

        Returns:
            GCS path in format gs://bucket/layer/file
        """
        path = f"gs://{bucket_name}/{layer_name}/"
        if file_name:
            path += file_name
        return path

    def get_file_list_in_layer(self, bucket_name: str, layer_name: str) -> list:
        """
        Retrieve list of file paths from a GCS bucket layer.

        Args:
            bucket_name: Name of the GCS bucket
            layer_name: Layer prefix to filter files

        Returns:
            List of file paths in format gs://bucket/blob_name
        """
        if not self.enabled:
            logger.warning("GCS not enabled. Cannot list files.")
            return []

        try:
            bucket = self.storage_client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=layer_name)
            blobs_list = [f"gs://{bucket_name}/{blob.name}" for blob in blobs]
            return blobs_list
        except Exception as e:
            logger.error(f"Error listing files from GCS: {str(e)}")
            return []

    def save_json_to_gcs(
        self,
        data: dict,
        bucket_name: str,
        layer_name: str,
        file_name: str,
    ) -> bool:
        """
        Save JSON data to GCS bucket.

        Args:
            data: Dictionary to save as JSON
            bucket_name: Name of the GCS bucket
            layer_name: Layer name for organization
            file_name: Name of the file to create

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            logger.debug("GCS not enabled. Skipping GCS save.")
            return False

        try:
            blob_path = f"{layer_name}/{file_name}"
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            blob.upload_from_string(
                json.dumps(data, indent=2),
                content_type="application/json"
            )
            logger.info(f"Successfully saved to GCS: gs://{bucket_name}/{blob_path}")
            return True

        except Exception as e:
            logger.error(f"Error saving to GCS: {str(e)}")
            return False

    def save_parquet_to_gcs(
        self,
        local_path: str,
        bucket_name: str,
        layer_name: str,
        folder_name: str,
    ) -> bool:
        """
        Upload parquet files from local path to GCS.

        Note: For Spark DataFrames, it's better to write directly to gs:// paths.
        This method is for uploading already-written local parquet files.

        Args:
            local_path: Local path containing parquet files
            bucket_name: Name of the GCS bucket
            layer_name: Layer name
            folder_name: Folder name in GCS

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            logger.debug("GCS not enabled. Skipping GCS upload.")
            return False

        try:
            bucket = self.storage_client.bucket(bucket_name)
            local_path_obj = Path(local_path)

            # Upload all parquet files
            for parquet_file in local_path_obj.rglob("*.parquet"):
                relative_path = parquet_file.relative_to(local_path_obj)
                blob_path = f"{layer_name}/{folder_name}/{relative_path}"

                blob = bucket.blob(blob_path)
                blob.upload_from_filename(str(parquet_file))
                logger.debug(f"Uploaded {parquet_file.name} to gs://{bucket_name}/{blob_path}")

            logger.info(f"Successfully uploaded parquet files to GCS: gs://{bucket_name}/{layer_name}/{folder_name}")
            return True

        except Exception as e:
            logger.error(f"Error uploading parquet to GCS: {str(e)}")
            return False

    def clear_bucket_layer(self, bucket_name: str, layer_name: str) -> bool:
        """
        Delete all blobs in a GCS bucket layer.

        Args:
            bucket_name: Name of the GCS bucket
            layer_name: Layer prefix to delete

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            logger.debug("GCS not enabled. Skipping GCS cleanup.")
            return False

        try:
            bucket = self.storage_client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=layer_name)

            for blob in blobs:
                blob.delete()
                logger.debug(f"Deleted gs://{bucket_name}/{blob.name}")

            logger.info(f"Successfully cleared GCS layer: gs://{bucket_name}/{layer_name}")
            return True

        except Exception as e:
            logger.error(f"Error clearing GCS bucket: {str(e)}")
            return False
