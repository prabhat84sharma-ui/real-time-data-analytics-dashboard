"""Configuration management for the Real-Time Data Analytics Dashboard Pipeline.

Handles environment variable loading, validation, and structured logging
setup for database, stream processor, data generator, and dashboard services.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """PostgreSQL Connection and Pooling Configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(default="localhost", alias="POSTGRES_HOST")
    port: int = Field(default=5432, alias="POSTGRES_PORT")
    database: str = Field(default="realtime_analytics", alias="POSTGRES_DB")
    user: str = Field(default="postgres", alias="POSTGRES_USER")
    password: str = Field(default="postgres", alias="POSTGRES_PASSWORD")
    min_pool_size: int = Field(default=2, alias="POSTGRES_MIN_POOL")
    max_pool_size: int = Field(default=10, alias="POSTGRES_MAX_POOL")
    connect_timeout: int = Field(default=10, alias="POSTGRES_TIMEOUT")

    @property
    def connection_url(self) -> str:
        """Construct PostgreSQL connection URI."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def dsn_dict(self) -> dict[str, object]:
        """Dictionary representation suitable for psycopg2 / asyncpg."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connect_timeout,
        }


class GeneratorSettings(BaseSettings):
    """Event Stream Generator Simulation Configuration."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    events_per_second: float = Field(default=15.0, alias="GEN_EVENTS_PER_SEC")
    burst_probability: float = Field(default=0.08, alias="GEN_BURST_PROB")
    burst_multiplier: float = Field(default=4.0, alias="GEN_BURST_MULT")
    anomaly_probability: float = Field(default=0.03, alias="GEN_ANOMALY_PROB")
    random_seed: Optional[int] = Field(default=42, alias="GEN_SEED")


class ProcessorSettings(BaseSettings):
    """Stream Processor & Ingestion Pipeline Configuration."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    batch_size: int = Field(default=50, alias="PROC_BATCH_SIZE")
    flush_interval_seconds: float = Field(default=2.0, alias="PROC_FLUSH_INTERVAL")
    window_1m_seconds: int = Field(default=60, alias="PROC_WINDOW_1M")
    window_5m_seconds: int = Field(default=300, alias="PROC_WINDOW_5M")
    enable_dlq: bool = Field(default=True, alias="PROC_ENABLE_DLQ")
    high_value_threshold: float = Field(default=1500.0, alias="PROC_HIGH_VALUE_THRESHOLD")


class DashboardSettings(BaseSettings):
    """Streamlit Visualization Settings."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    refresh_interval_ms: int = Field(default=3000, alias="DASHBOARD_REFRESH_MS")
    history_window_minutes: int = Field(default=30, alias="DASHBOARD_HISTORY_MINS")
    page_title: str = "Real-Time E-Commerce Telemetry & Analytics"
    theme_primary_color: str = "#00B4D8"


class AppSettings(BaseSettings):
    """Global Application Unified Settings."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    env: str = Field(default="development", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    generator: GeneratorSettings = Field(default_factory=GeneratorSettings)
    processor: ProcessorSettings = Field(default_factory=ProcessorSettings)
    dashboard: DashboardSettings = Field(default_factory=DashboardSettings)

    def setup_logging(self) -> logging.Logger:
        """Initialize structured root and module logging."""
        numeric_level = getattr(logging, self.log_level.upper(), logging.INFO)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s:%(lineno)d] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.setLevel(numeric_level)
        root_logger.handlers = [handler]

        return logging.getLogger("realtime_analytics")


# Global singleton settings instance
settings = AppSettings()
logger = settings.setup_logging()
