import json
import logging

from tasks.celery_app import celery_app
from core.redis_client import get_redis
from core.config import settings

logger = logging.getLogger(__name__)


@celery_app.task(
    name="exec.tasks.persist_history",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
)
def persist_history(self, session_id: str, record: dict):
    """
    Fire-and-forget: Append an execution record to the Redis history list.
    Capped at HISTORY_CAP entries per session using lpush + ltrim.
    Sets a 24-hour TTL on the list key.

    Args:
        session_id: The user's session ID.
        record: Dict containing sql, executed_at, execution_time_ms, row_count, success, error.
    """
    try:
        key = f"exec_history:{session_id}"
        r = get_redis()
        r.lpush(key, json.dumps(record))
        r.ltrim(key, 0, settings.HISTORY_CAP - 1)
        r.expire(key, 86400)  # 24h TTL
        logger.debug("persist_history: session=%s key=%s", session_id, key)
    except Exception as exc:
        logger.error("persist_history failed: %s", exc)
        raise self.retry(exc=exc)


@celery_app.task(
    name="exec.tasks.archive_result",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
)
def archive_result(self, session_id: str, sql_hash: str, result_json: str):
    """
    Fire-and-forget: Store the full paginated result in Redis with a short TTL.
    Allows re-pagination requests to avoid re-executing the same query.

    Args:
        session_id: The user's session ID.
        sql_hash: Short hash of the SQL (first 16 chars of SHA256).
        result_json: JSON string of the full ExecutionResult.
    """
    try:
        key = f"exec_result:{session_id}:{sql_hash}"
        get_redis().setex(key, 300, result_json)  # 5-minute TTL
        logger.debug("archive_result: stored key=%s (TTL=300s)", key)
    except Exception as exc:
        logger.error("archive_result failed: %s", exc)
        raise self.retry(exc=exc)
