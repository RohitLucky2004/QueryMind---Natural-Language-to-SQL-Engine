# querymind-schema-service/services/cache.py
"""
Redis schema cache for the Schema Service.

Key layout:
  schema:{session_id}:{table_name}   → JSON string of TableInfo dict
  schema:{session_id}:__index__      → JSON list of all table names cached for session

TTL: SCHEMA_CACHE_TTL seconds (default 3600).
"""

import json
import logging
from datetime import datetime, timezone

import redis

from core.config import settings
from core.redis_client import get_redis
from models.schema_models import TableInfo

logger = logging.getLogger(__name__)

_TTL = settings.SCHEMA_CACHE_TTL
_INDEX_SUFFIX = "__index__"


def _table_key(session_id: str, table_name: str) -> str:
    return f"schema:{session_id}:{table_name}"


def _index_key(session_id: str) -> str:
    return f"schema:{session_id}:{_INDEX_SUFFIX}"


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

def store_table_chunk(session_id: str, table: TableInfo, ttl: int = _TTL) -> None:
    """Serialize a TableInfo and store it in Redis under its chunk key."""
    r = get_redis()
    key = _table_key(session_id, table.table_name)
    payload = json.dumps(table.model_dump())
    r.setex(key, ttl, payload)
    logger.debug("Cached table chunk: %s (TTL=%ds)", key, ttl)


def store_schema_chunks(session_id: str, tables: list[TableInfo], ttl: int = _TTL) -> None:
    """Store all table chunks and update the index key atomically via pipeline."""
    r = get_redis()
    table_names = [t.table_name for t in tables]

    pipe = r.pipeline(transaction=False)
    for table in tables:
        key = _table_key(session_id, table.table_name)
        pipe.setex(key, ttl, json.dumps(table.model_dump()))

    idx_key = _index_key(session_id)
    pipe.setex(idx_key, ttl, json.dumps(table_names))
    pipe.execute()
    logger.info("Stored %d schema chunks for session %s", len(tables), session_id)


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------

def get_cached_table(session_id: str, table_name: str) -> dict | None:
    """Return the cached dict for a single table, or None on miss."""
    r = get_redis()
    raw = r.get(_table_key(session_id, table_name))
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("JSON decode error for %s/%s: %s", session_id, table_name, exc)
        return None


def get_cached_tables(session_id: str, table_names: list[str] | None = None) -> dict[str, dict]:
    """
    Return a mapping of {table_name: table_dict} from cache.
    If table_names is None or empty, return all cached tables for the session.
    """
    r = get_redis()

    if not table_names:
        # Fetch the index to know which tables are cached
        raw_idx = r.get(_index_key(session_id))
        if raw_idx is None:
            return {}
        try:
            table_names = json.loads(raw_idx)
        except json.JSONDecodeError:
            return {}

    keys = [_table_key(session_id, t) for t in table_names]
    values = r.mget(keys)

    result: dict[str, dict] = {}
    for tname, raw in zip(table_names, values):
        if raw is not None:
            try:
                result[tname] = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Bad JSON in cache for %s/%s", session_id, tname)
    return result


def get_index(session_id: str) -> list[str]:
    """Return the list of table names cached for this session (from index key)."""
    r = get_redis()
    raw = r.get(_index_key(session_id))
    if raw is None:
        return []
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return []


def get_ttl_remaining(session_id: str) -> int:
    """Return TTL (seconds) of the index key, or -1 if missing."""
    r = get_redis()
    ttl = r.ttl(_index_key(session_id))
    return ttl  # -2 if key missing, -1 if no TTL set


# ---------------------------------------------------------------------------
# Invalidate
# ---------------------------------------------------------------------------

def invalidate_session(session_id: str) -> int:
    """Delete all cached chunks and the index for a session. Returns count of keys deleted."""
    r = get_redis()
    idx_names = get_index(session_id)

    keys = [_table_key(session_id, t) for t in idx_names]
    keys.append(_index_key(session_id))

    if not keys:
        return 0

    deleted = r.delete(*keys)
    logger.info("Invalidated %d cache keys for session %s", deleted, session_id)
    return deleted


# ---------------------------------------------------------------------------
# Expiry scan (used by Celery beat task)
# ---------------------------------------------------------------------------

def find_expiring_sessions(ttl_threshold: int = 300) -> list[str]:
    """
    Scan Redis for schema index keys with TTL < ttl_threshold seconds.
    Returns a list of session_ids whose caches are about to expire.
    """
    r = get_redis()
    expiring: list[str] = []

    cursor = 0
    pattern = "schema:*:__index__"
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=100)
        for key in keys:
            ttl = r.ttl(key)
            if 0 < ttl < ttl_threshold:
                # Extract session_id from key: schema:{session_id}:__index__
                parts = key.split(":")
                if len(parts) >= 3:
                    session_id = parts[1]
                    expiring.append(session_id)
        if cursor == 0:
            break

    return expiring
