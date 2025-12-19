"""Configuration management for the brewery data pipeline."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class APIConfig:
    """Configuration for Open Brewery DB API."""

    base_url: str = "https://api.openbrewerydb.org/v1/breweries"
    per_page: int = 200
    timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 5


@dataclass
class DataLakeConfig:
    """Configuration for data lake paths following medallion architecture."""

    bronze_path: str = "/opt/airflow/data/bronze"
    silver_path: str = "/opt/airflow/data/silver"
    gold_path: str = "/opt/airflow/data/gold"
    gcs_bucket: Optional[str] = None

    def __post_init__(self):
        """Override with environment variables if available."""
        self.bronze_path = os.getenv("BRONZE_LAYER_PATH", self.bronze_path)
        self.silver_path = os.getenv("SILVER_LAYER_PATH", self.silver_path)
        self.gold_path = os.getenv("GOLD_LAYER_PATH", self.gold_path)
        self.gcs_bucket = os.getenv("GCS_BUCKET_NAME", self.gcs_bucket)


@dataclass
class PipelineConfig:
    """Main pipeline configuration."""

    api: APIConfig
    datalake: DataLakeConfig
    environment: str = "production"

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Create configuration from environment variables."""
        api_config = APIConfig(
            base_url=os.getenv("BREWERY_API_BASE_URL", APIConfig.base_url),
            per_page=int(os.getenv("BREWERY_API_PER_PAGE", APIConfig.per_page)),
        )
        datalake_config = DataLakeConfig()
        environment = os.getenv("ENVIRONMENT", "production")

        return cls(api=api_config, datalake=datalake_config, environment=environment)


def get_config() -> PipelineConfig:
    """Get pipeline configuration singleton."""
    return PipelineConfig.from_env()
