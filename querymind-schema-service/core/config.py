# querymind-schema-service/core/config.py
"""
Environment-based configuration for the Schema Service.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Server
    PORT: int = 8001
    LOG_LEVEL: str = "INFO"

    # RabbitMQ
    AMQP_URL: str = "amqp://guest:guest@localhost:5672/"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # Schema cache TTL in seconds
    SCHEMA_CACHE_TTL: int = 3600

    # Celery beat period for cache refresh (seconds)
    CACHE_REFRESH_INTERVAL: int = 3300  # 55 minutes

    # Connection pool size for user-provided DBs
    DB_POOL_SIZE: int = 5
    DB_POOL_TIMEOUT: int = 30


settings = Settings()
