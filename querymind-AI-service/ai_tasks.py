import json
import logging
from datetime import datetime, timezone

from tasks.celery_app import celery_app
from core.redis_client import get_redis

logger = logging.getLogger(__name__)


@celery_app.task(name="ai.tasks.cache_result", bind=True, max_retries=3, default_retry_delay=5)
def cache_result(self, cache_key: str, result_json: str, ttl: int = 86400):
    """
    Fire-and-forget: Store the generated SQL result in Redis cache.

    Args:
        cache_key: Redis key (query_cache:{sha256}).
        result_json: JSON string of the generation result.
        ttl: Cache TTL in seconds (default 24h).
    """
    try:
        get_redis().setex(cache_key, ttl, result_json)
        logger.info("cache_result: stored key=%s ttl=%ds", cache_key, ttl)
    except Exception as exc:
        logger.error("cache_result failed: %s", exc)
        raise self.retry(exc=exc)


@celery_app.task(name="ai.tasks.log_usage", bind=True, max_retries=3, default_retry_delay=5)
def log_usage(self, session_id: str, question: str, tokens_used: int, latency_ms: int):
    """
    Fire-and-forget: Log AI usage metrics (tokens, latency) to Redis per session.
    Keeps the last 100 entries per session using lpush + ltrim.

    Args:
        session_id: The user's session ID.
        question: The original question (for audit).
        tokens_used: Total tokens consumed (input + output).
        latency_ms: End-to-end generation latency in milliseconds.
    """
    try:
        entry = json.dumps({
            "question": question[:500],  # cap for storage efficiency
            "tokens": tokens_used,
            "latency_ms": latency_ms,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        r = get_redis()
        r.lpush(f"usage_log:{session_id}", entry)
        r.ltrim(f"usage_log:{session_id}", 0, 99)
        logger.debug(
            "log_usage: session=%s tokens=%d latency=%dms",
            session_id,
            tokens_used,
            latency_ms,
        )
    except Exception as exc:
        logger.error("log_usage failed: %s", exc)
        raise self.retry(exc=exc)
