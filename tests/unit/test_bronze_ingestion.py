"""Unit tests for bronze layer ingestion."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from src.bronze.ingestion import BreweryAPIClient, ingest_breweries


@pytest.mark.unit
class TestBreweryAPIClient:
    """Test cases for BreweryAPIClient."""

    def test_client_initialization(self, api_config):
        """Test API client initialization."""
        client = BreweryAPIClient(api_config)

        assert client.config == api_config
        assert client.session is not None

    def test_fetch_breweries_success(self, api_config, sample_brewery_data):
        """Test successful brewery fetch."""
        client = BreweryAPIClient(api_config)

        with patch.object(client.session, "get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = sample_brewery_data
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            result = client.fetch_breweries(page=1, per_page=50)

            assert len(result) == len(sample_brewery_data)
            assert result[0]["name"] == "Test Brewery 1"
            mock_get.assert_called_once()

    def test_fetch_breweries_api_error(self, api_config):
        """Test handling of API errors."""
        client = BreweryAPIClient(api_config)

        with patch.object(client.session, "get") as mock_get:
            mock_get.side_effect = requests.RequestException("API Error")

            with pytest.raises(requests.RequestException):
                client.fetch_breweries(page=1)

    def test_fetch_all_breweries(self, api_config, sample_brewery_data):
        """Test fetching all breweries with pagination."""
        client = BreweryAPIClient(api_config)

        # Mock multiple pages
        with patch.object(client, "fetch_breweries") as mock_fetch:
            # First page returns data, second page returns empty
            mock_fetch.side_effect = [sample_brewery_data, []]

            result = client.fetch_all_breweries()

            assert len(result) == len(sample_brewery_data)
            assert mock_fetch.call_count == 2

    def test_fetch_all_breweries_last_page_detection(self, api_config):
        """Test detection of last page when less than per_page results."""
        client = BreweryAPIClient(api_config)

        partial_data = [{"id": "1", "name": "Brewery"}]  # Less than per_page

        with patch.object(client, "fetch_breweries") as mock_fetch:
            mock_fetch.return_value = partial_data

            result = client.fetch_all_breweries()

            assert len(result) == 1
            # Should only call once since partial page indicates last page
            assert mock_fetch.call_count == 1


@pytest.mark.unit
class TestIngestBreweries:
    """Test cases for ingest_breweries function."""

    def test_ingest_breweries_success(
        self, api_config, temp_data_dir, sample_brewery_data
    ):
        """Test successful brewery ingestion."""
        client = BreweryAPIClient(api_config)

        with patch.object(client, "fetch_all_breweries") as mock_fetch:
            mock_fetch.return_value = sample_brewery_data

            output_path = str(temp_data_dir / "bronze")
            result = ingest_breweries(
                api_client=client,
                output_path=output_path,
                execution_date="2024-01-01",
            )

            # Verify file was created
            assert Path(result).exists()

            # Verify content
            with open(result, "r") as f:
                data = json.load(f)
                assert len(data) == len(sample_brewery_data)

    def test_ingest_breweries_no_data(self, api_config, temp_data_dir):
        """Test handling of empty API response."""
        client = BreweryAPIClient(api_config)

        with patch.object(client, "fetch_all_breweries") as mock_fetch:
            mock_fetch.return_value = []

            output_path = str(temp_data_dir / "bronze")

            with pytest.raises(ValueError, match="No data received from API"):
                ingest_breweries(
                    api_client=client,
                    output_path=output_path,
                    execution_date="2024-01-01",
                )

    def test_ingest_breweries_creates_metadata(
        self, api_config, temp_data_dir, sample_brewery_data
    ):
        """Test that metadata file is created."""
        client = BreweryAPIClient(api_config)

        with patch.object(client, "fetch_all_breweries") as mock_fetch:
            mock_fetch.return_value = sample_brewery_data

            output_path = str(temp_data_dir / "bronze")
            ingest_breweries(
                api_client=client,
                output_path=output_path,
                execution_date="2024-01-01",
            )

            # Check metadata file exists
            metadata_files = list(
                (temp_data_dir / "bronze" / "date=2024-01-01").glob("metadata_*.json")
            )
            assert len(metadata_files) == 1

            # Verify metadata content
            with open(metadata_files[0], "r") as f:
                metadata = json.load(f)
                assert metadata["record_count"] == len(sample_brewery_data)
                assert metadata["execution_date"] == "2024-01-01"
