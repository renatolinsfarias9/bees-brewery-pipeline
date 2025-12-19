"""Pytest configuration and fixtures."""

import json
import tempfile
from pathlib import Path
from typing import Dict, List

import pytest


@pytest.fixture
def sample_brewery_data() -> List[Dict]:
    """Sample brewery data for testing."""
    return [
        {
            "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
            "name": "Test Brewery 1",
            "brewery_type": "micro",
            "address_1": "123 Main St",
            "city": "San Francisco",
            "state_province": "California",
            "postal_code": "94102",
            "country": "United States",
            "longitude": "-122.419906",
            "latitude": "37.774929",
            "phone": "4155551234",
            "website_url": "http://testbrewery1.com",
            "state": "California",
            "street": "123 Main St",
        },
        {
            "id": "9c5a66c8-cc13-416f-a5d9-0a769c87d318",
            "name": "Test Brewery 2",
            "brewery_type": "regional",
            "address_1": "456 Oak Ave",
            "city": "Portland",
            "state_province": "Oregon",
            "postal_code": "97201",
            "country": "United States",
            "longitude": "-122.676207",
            "latitude": "45.520247",
            "phone": "",
            "website_url": "",
            "state": "Oregon",
            "street": "456 Oak Ave",
        },
        {
            "id": "34e8c68b-6146-453f-a4b9-1f6cd99a5ada",
            "name": "Test Brewery 3",
            "brewery_type": "micro",
            "address_1": None,
            "city": "Denver",
            "state_province": "Colorado",
            "postal_code": "80202",
            "country": "United States",
            "longitude": None,
            "latitude": None,
            "phone": "3035551234",
            "website_url": "http://testbrewery3.com",
            "state": "Colorado",
            "street": None,
        },
    ]


@pytest.fixture
def temp_data_dir():
    """Create temporary directory for test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)

        # Create layer directories
        (temp_path / "bronze").mkdir()
        (temp_path / "silver").mkdir()
        (temp_path / "gold").mkdir()

        yield temp_path


@pytest.fixture
def bronze_test_file(temp_data_dir, sample_brewery_data):
    """Create a test bronze layer JSON file."""
    execution_date = "2024-01-01"
    bronze_path = temp_data_dir / "bronze" / f"date={execution_date}"
    bronze_path.mkdir(parents=True)

    test_file = bronze_path / "breweries_test.json"
    with open(test_file, "w") as f:
        json.dump(sample_brewery_data, f)

    return test_file


@pytest.fixture
def api_config():
    """Test API configuration."""
    from src.utils.config import APIConfig

    return APIConfig(
        base_url="https://api.openbrewerydb.org/v1/breweries",
        per_page=50,
        timeout=10,
        max_retries=2,
    )


@pytest.fixture
def datalake_config(temp_data_dir):
    """Test data lake configuration."""
    from src.utils.config import DataLakeConfig

    return DataLakeConfig(
        bronze_path=str(temp_data_dir / "bronze"),
        silver_path=str(temp_data_dir / "silver"),
        gold_path=str(temp_data_dir / "gold"),
    )
