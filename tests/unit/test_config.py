"""Unit tests for configuration management."""

import os

import pytest

from src.utils.config import APIConfig, DataLakeConfig, PipelineConfig, get_config


@pytest.mark.unit
class TestAPIConfig:
    """Test cases for APIConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = APIConfig()

        assert config.base_url == "https://api.openbrewerydb.org/v1/breweries"
        assert config.per_page == 200
        assert config.timeout == 30
        assert config.max_retries == 3

    def test_custom_values(self):
        """Test custom configuration values."""
        config = APIConfig(
            base_url="http://custom-url.com",
            per_page=100,
            timeout=60,
            max_retries=5,
        )

        assert config.base_url == "http://custom-url.com"
        assert config.per_page == 100
        assert config.timeout == 60
        assert config.max_retries == 5


@pytest.mark.unit
class TestDataLakeConfig:
    """Test cases for DataLakeConfig."""

    def test_default_paths(self):
        """Test default data lake paths."""
        config = DataLakeConfig()

        assert "/bronze" in config.bronze_path
        assert "/silver" in config.silver_path
        assert "/gold" in config.gold_path

    def test_environment_override(self, monkeypatch):
        """Test environment variable override."""
        monkeypatch.setenv("BRONZE_LAYER_PATH", "/custom/bronze")
        monkeypatch.setenv("SILVER_LAYER_PATH", "/custom/silver")
        monkeypatch.setenv("GOLD_LAYER_PATH", "/custom/gold")

        config = DataLakeConfig()

        assert config.bronze_path == "/custom/bronze"
        assert config.silver_path == "/custom/silver"
        assert config.gold_path == "/custom/gold"


@pytest.mark.unit
class TestPipelineConfig:
    """Test cases for PipelineConfig."""

    def test_from_env_default(self):
        """Test creating config from environment with defaults."""
        config = PipelineConfig.from_env()

        assert isinstance(config.api, APIConfig)
        assert isinstance(config.datalake, DataLakeConfig)
        assert config.environment == "production"

    def test_from_env_custom(self, monkeypatch):
        """Test creating config from custom environment variables."""
        monkeypatch.setenv("BREWERY_API_BASE_URL", "http://test-api.com")
        monkeypatch.setenv("BREWERY_API_PER_PAGE", "50")
        monkeypatch.setenv("ENVIRONMENT", "development")

        config = PipelineConfig.from_env()

        assert config.api.base_url == "http://test-api.com"
        assert config.api.per_page == 50
        assert config.environment == "development"

    def test_get_config(self):
        """Test get_config function."""
        config = get_config()

        assert isinstance(config, PipelineConfig)
        assert isinstance(config.api, APIConfig)
        assert isinstance(config.datalake, DataLakeConfig)
