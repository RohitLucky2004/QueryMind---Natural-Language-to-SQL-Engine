import redis
from core.config import settings

_redis_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
        )
    return _redis_client


def cache_get(key: str) -> str | None:
    return get_redis().get(key)


def cache_set(key: str, value: str, ttl: int = 86400) -> None:
    get_redis().setex(key, ttl, value)


def usage_log_push(session_id: str, entry: str) -> None:
    r = get_redis()
    r.lpush(f"usage_log:{session_id}", entry)
    r.ltrim(f"usage_log:{session_id}", 0, 99)
