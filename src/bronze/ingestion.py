"""
Bronze Layer - Raw data ingestion from Open Brewery DB API.

This module handles fetching brewery data from the API and persisting it
in raw format (JSON) to the bronze layer of the data lake.
Supports both local storage and Google Cloud Storage (GCS) for cloud deployment.
"""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.config import APIConfig
from utils.logger import setup_logger
from utils.gcs_storage import GcsStorage

logger = setup_logger(__name__)


class BreweryAPIClient:
    """Client for interacting with the Open Brewery DB API."""

    def __init__(self, config: APIConfig):
        """
        Initialize the API client.

        Args:
            config: API configuration object
        """
        self.config = config
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry logic.

        Returns:
            Configured requests session
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def fetch_breweries(
        self, page: int = 1, per_page: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch breweries from the API with pagination.

        Args:
            page: Page number to fetch
            per_page: Number of records per page

        Returns:
            List of brewery records

        Raises:
            requests.RequestException: If API request fails
        """
        per_page = per_page or self.config.per_page

        params = {"page": page, "per_page": per_page}

        try:
            logger.info(f"Fetching breweries - Page {page}, Per page: {per_page}")
            response = self.session.get(
                self.config.base_url,
                params=params,
                timeout=self.config.timeout,
            )
            response.raise_for_status()

            data = response.json()
            logger.info(f"Successfully fetched {len(data)} breweries from page {page}")

            return data

        except requests.RequestException as e:
            logger.error(f"Error fetching breweries from page {page}: {str(e)}")
            raise

    def fetch_all_breweries(self) -> List[Dict[str, Any]]:
        """
        Fetch all breweries from the API by paginating through all pages.

        Returns:
            List of all brewery records

        Raises:
            requests.RequestException: If API request fails
        """
        all_breweries = []
        page = 1
        total_fetched = 0

        logger.info("Starting to fetch all breweries")

        while True:
            try:
                breweries = self.fetch_breweries(page=page)

                # Break if no more data
                if not breweries:
                    logger.info(f"No more data found at page {page}")
                    break

                all_breweries.extend(breweries)
                total_fetched += len(breweries)

                logger.info(
                    f"Fetched {len(breweries)} breweries from page {page}. "
                    f"Total so far: {total_fetched}"
                )

                # Check if we got less than per_page, indicating last page
                if len(breweries) < self.config.per_page:
                    logger.info("Reached last page (partial page received)")
                    break

                page += 1

                # Small delay to be respectful to the API
                time.sleep(0.5)

            except requests.RequestException as e:
                logger.error(f"Failed to fetch all breweries: {str(e)}")
                raise

        logger.info(f"Completed fetching all breweries. Total: {total_fetched}")
        return all_breweries


def ingest_breweries(
    api_client: BreweryAPIClient,
    output_path: str,
    execution_date: Optional[str] = None,
    gcs_bucket: Optional[str] = None,
) -> str:
    """Ingest brewery data from API and save to bronze layer (local + GCS)."""
    try:
        # Use provided execution date or current date
        exec_date = execution_date or datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        logger.info(f"Starting brewery data ingestion for date: {exec_date}")

        # Initialize GCS if bucket name provided
        gcs_storage = None
        if gcs_bucket:
            gcs_storage = GcsStorage()
            if gcs_storage.is_enabled():
                logger.info(f"GCS storage enabled for bucket: {gcs_bucket}")
            else:
                logger.info("GCS not available, will save locally only")

        # Fetch all breweries
        breweries = api_client.fetch_all_breweries()

        if not breweries:
            logger.warning("No breweries fetched from API")
            raise ValueError("No data received from API")

        # Create output directory with date partition
        output_dir = Path(output_path) / f"date={exec_date}"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save to local JSON file
        output_file = output_dir / f"breweries_{timestamp}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(breweries, f, indent=2, ensure_ascii=False)

        logger.info(
            f"Successfully saved {len(breweries)} breweries to local: {output_file}"
        )

        # Save to GCS if available
        if gcs_storage and gcs_storage.is_enabled():
            gcs_file = f"{exec_date}/breweries_{timestamp}.json"
            success = gcs_storage.save_json_to_gcs(
                data=breweries,
                bucket_name=gcs_bucket,
                layer_name="bronze",
                file_name=gcs_file,
            )
            if success:
                logger.info(f"Successfully saved to GCS: gs://{gcs_bucket}/bronze/{gcs_file}")

        # Save metadata
        metadata = {
            "execution_date": exec_date,
            "timestamp": timestamp,
            "record_count": len(breweries),
            "output_file": str(output_file),
            "gcs_enabled": gcs_storage.is_enabled() if gcs_storage else False,
            "gcs_bucket": gcs_bucket if gcs_storage and gcs_storage.is_enabled() else None,
        }

        metadata_file = output_dir / f"metadata_{timestamp}.json"
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Metadata saved to {metadata_file}")

        return str(output_file)

    except Exception as e:
        logger.error(f"Error during brewery ingestion: {str(e)}")
        raise
