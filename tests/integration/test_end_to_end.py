"""End-to-end integration tests for the brewery pipeline."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.bronze.ingestion import BreweryAPIClient, ingest_breweries
from src.utils.config import APIConfig


@pytest.mark.integration
@pytest.mark.slow
class TestEndToEndPipeline:
    """End-to-end integration tests."""

    def test_bronze_ingestion_integration(
        self, api_config, temp_data_dir, sample_brewery_data
    ):
        """Test bronze layer ingestion with mocked API."""
        client = BreweryAPIClient(api_config)

        with patch.object(client, "fetch_all_breweries") as mock_fetch:
            mock_fetch.return_value = sample_brewery_data

            output_path = str(temp_data_dir / "bronze")
            result_file = ingest_breweries(
                api_client=client,
                output_path=output_path,
                execution_date="2024-01-01",
            )

            # Verify bronze layer structure
            assert Path(result_file).exists()

            # Verify data integrity
            with open(result_file, "r") as f:
                data = json.load(f)
                assert len(data) == 3
                assert all("id" in record for record in data)
                assert all("name" in record for record in data)

            # Verify metadata
            metadata_dir = Path(result_file).parent
            metadata_files = list(metadata_dir.glob("metadata_*.json"))
            assert len(metadata_files) == 1

    def test_data_partitioning(self, temp_data_dir, sample_brewery_data):
        """Test that data is properly partitioned by execution date."""
        api_config = APIConfig()
        client = BreweryAPIClient(api_config)

        with patch.object(client, "fetch_all_breweries") as mock_fetch:
            mock_fetch.return_value = sample_brewery_data

            output_path = str(temp_data_dir / "bronze")

            # Ingest for different dates
            ingest_breweries(client, output_path, "2024-01-01")
            ingest_breweries(client, output_path, "2024-01-02")

            # Verify partitions exist
            bronze_path = Path(output_path)
            assert (bronze_path / "date=2024-01-01").exists()
            assert (bronze_path / "date=2024-01-02").exists()

            # Verify each partition has data
            assert len(list((bronze_path / "date=2024-01-01").glob("*.json"))) > 0
            assert len(list((bronze_path / "date=2024-01-02").glob("*.json"))) > 0
