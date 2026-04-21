"""Application configuration loaded from environment variables.

Uses pydantic-settings for type-safe config with validation.
All settings are loaded from a .env file or environment variables
with the prefix ARGOS_.

Usage:
    from argos.config import settings

    db_url = settings.database_url
    scraper_delay = settings.scraper.delay_seconds
"""

from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# ============================================================
# Enums for type-safe string values
# ============================================================


class Environment(StrEnum):
    """Allowed values for the deployment environment."""

    DEV = "dev"
    STAGING = "staging"
    PRODUCTION = "production"


class LogLevel(StrEnum):
    """Allowed log levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


# ============================================================
# Nested settings groups
# ============================================================


class ScraperSettings(BaseSettings):
    """Configuration for the web scrappers."""

    model_config = SettingsConfigDict(env_prefix="ARGOS_SCRAPER_")

    delay_seconds: int = Field(
        default=5,
        ge=1,
        le=60,
        description="Delay between requests. Respectful scraping.",
    )
    user_agent: str = Field(
        default="ARGOS-Bot/0.1 (Research project)",
        description="User-Agent header for all requests.",
    )
    concurrency: int = Field(default=2, ge=1, le=10, description="Max concurrent requests.")


# ============================================================
# Main settings class
# ============================================================


class Settings(BaseSettings):
    """Main configuration for ARGOS.

    Load from .env in the root of the project or from OS environment variables
    with the prefix ARGOS_. Fail fast if a required variable is missing or invalid.
    """

    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.parent.parent / ".env",
        env_file_encoding="utf-8",
        env_prefix="ARGOS_",
        case_sensitive=False,
        extra="ignore",
    )

    env: Environment = Field(default=Environment.DEV, description="Deployment environment.")
    log_level: LogLevel = Field(default=LogLevel.INFO, description="Python logging level.")

    db_host: str = Field(default="localhost", description="Postgres host")
    db_port: int = Field(default=5432, ge=1, le=65535, description="Postgres port.")
    db_name: str = Field(default="argos", description="Database name.")
    db_user: str = Field(default="argos", description="Database user.")
    db_password: SecretStr = Field(
        default=SecretStr("change_me"),
        description="Database password. Wrapped in SecretStr.",
    )

    s3_endpoint_url: str = Field(
        default="http://localhost:9000",
        description="S3 endpoint (MinIO local or R2 production).",
    )
    s3_access_key: SecretStr = Field(
        default=SecretStr("minioadmin"),
        description="S3 access key.",
    )
    s3_secret_key: SecretStr = Field(
        default=SecretStr("minioadmin"),
        description="S3 secret key.",
    )
    s3_bucket: str = Field(default="argos-data", description="Bucket name.")
    s3_region: str = Field(default="auto", description="S3 region.")

    mlflow_tracking_url: str = Field(
        default="http://localhost:5000",
        description="MLflow tracking server URI.",
    )

    scraper: ScraperSettings = Field(default_factory=ScraperSettings)

    # ============================================================
    # Computed properties
    # ============================================================

    @property
    def database_url(self) -> str:
        """Async Postgres URL (asyncpg driver)for SQLAlchemy."""
        password = self.db_password.get_secret_value()
        return (
            f"postgresql+asyncpg://{self.db_user}:{password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def database_url_sync(self) -> str:
        """Sync Postgres URL (psycopg driver) for non-async contexts."""
        password = self.db_password.get_secret_value()
        return (
            f"postgresql+psycopg://{self.db_user}:{password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def is_production(self) -> bool:
        """Helper for production-specific logic."""
        return self.env == Environment.PRODUCTION

    # ============================================================
    # Validators
    # ============================================================

    @field_validator("db_password")
    @classmethod
    def _reject_default_password_in_prod(cls, v: SecretStr) -> SecretStr:
        """Prevent default password in production."""
        if v.get_secret_value() == "change_me":
            pass
        return v

    def model_post_init(self, __context: object) -> None:
        """Run after all fields are validated."""
        if self.is_production:
            insecure_defaults = {
                "db_password": "change_me",
                "s3_access_key": "minioadmin",
                "s3_secret_key": "minioadmin",
            }
            for field_name, default_value in insecure_defaults.items():
                value = getattr(self, field_name)
                if isinstance(value, SecretStr):
                    value = value.get_secret_value()
                if value == default_value:
                    raise ValueError(
                        f"Insecure default value for '{field_name}' detected "
                        f"in production environment. Set a real value."
                    )


# ============================================================
# Singleton accessor
# ============================================================


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Returns cached singleton of Settings.

    The @lru_cache ensures that the config is read from .env
    only once per process lifetime.
    """
    return Settings()


settings = get_settings()
