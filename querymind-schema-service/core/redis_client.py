# querymind-schema-service/core/redis_client.py
"""
Redis connection singleton for the Schema Service.
"""

import logging
import redis
from functools import lru_cache
from core.config import settings

logger = logging.getLogger(__name__)


class RedisClient:
    """Thread-safe Redis client singleton."""

    _instance: redis.Redis | None = None

    @classmethod
    def get(cls) -> redis.Redis:
        if cls._instance is None:
            cls._instance = redis.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30,
            )
            logger.info("Redis client initialized: %s", settings.REDIS_URL)
        return cls._instance

    @classmethod
    def ping(cls) -> bool:
        try:
            return cls.get().ping()
        except Exception as exc:
            logger.error("Redis ping failed: %s", exc)
            return False

    @classmethod
    def close(cls) -> None:
        if cls._instance is not None:
            cls._instance.close()
            cls._instance = None
            logger.info("Redis connection closed.")


def get_redis() -> redis.Redis:
    """Shorthand accessor used throughout the service."""
    return RedisClient.get()
