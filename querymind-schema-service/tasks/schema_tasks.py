# querymind-schema-service/tasks/schema_tasks.py
"""
Celery tasks for the Schema Service.

  schema.tasks.warm_cache              - Pre-build all Redis chunks after connect.
  schema.tasks.refresh_expiring_caches - Periodic: re-warm sessions about to expire.
"""

import logging
import time
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from tasks.celery_app import celery_app
from services.introspector import introspect_database
from services.cache import store_schema_chunks, find_expiring_sessions, get_index
from core.config import settings
from core.session_store import session_store

logger = logging.getLogger(__name__)


@celery_app.task(
    name="schema.tasks.warm_cache",
    bind=True,
    max_retries=3,
    default_retry_delay=10,
    queue="schema-celery",
)
def warm_cache(self, session_id: str, connection_string: str) -> dict:
    """
    Background task: introspect the database and pre-build all schema chunks in Redis.

    Triggered after a successful connect reply so that the connect response is not
    delayed by the potentially slow full-schema introspection.
    """
    start = time.monotonic()
    logger.info("[warm_cache] Starting for session %s", session_id)

    try:
        # Create a temporary engine if the session is no longer in the in-memory store
        # (e.g. this task runs in a separate Celery worker process)
        entry = session_store.get(session_id)
        if entry is not None:
            engine = entry.engine
            database_name = entry.database_name
        else:
            logger.info(
                "[warm_cache] Session %s not in memory store; creating ephemeral engine",
                session_id,
            )
            engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_size=2,
                max_overflow=0,
                connect_args={"connect_timeout": settings.DB_POOL_TIMEOUT},
            )
            # Derive database name from the connection string
            database_name = connection_string.rsplit("/", 1)[-1].split("?")[0] or "unknown"

        db_schema = introspect_database(engine, database_name)
        store_schema_chunks(session_id, db_schema.tables, ttl=settings.SCHEMA_CACHE_TTL)

        elapsed_ms = int((time.monotonic() - start) * 1000)
        logger.info(
            "[warm_cache] Done for session %s — %d tables in %dms",
            session_id,
            db_schema.total_tables,
            elapsed_ms,
        )

        # If we created a temporary engine, dispose it
        if entry is None:
            engine.dispose()

        return {
            "session_id": session_id,
            "tables_cached": db_schema.total_tables,
            "elapsed_ms": elapsed_ms,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

    except OperationalError as exc:
        logger.error("[warm_cache] DB connection error for session %s: %s", session_id, exc)
        raise self.retry(exc=exc)

    except Exception as exc:
        logger.error("[warm_cache] Unexpected error for session %s: %s", session_id, exc)
        raise self.retry(exc=exc)


@celery_app.task(
    name="schema.tasks.refresh_expiring_caches",
    queue="schema-celery",
)
def refresh_expiring_caches() -> dict:
    """
    Periodic task (run by Celery Beat every 55 minutes).

    Scans Redis for schema index keys with TTL < 300 seconds.
    For each found session, re-triggers warm_cache if the session has a live DB engine
    in the in-memory store (i.e. the user is still connected).
    """
    logger.info("[refresh_expiring_caches] Scanning for expiring cache entries...")
    expiring = find_expiring_sessions(ttl_threshold=300)

    if not expiring:
        logger.info("[refresh_expiring_caches] No expiring sessions found.")
        return {"refreshed": 0, "skipped": 0}

    refreshed = 0
    skipped = 0

    for session_id in expiring:
        entry = session_store.get(session_id)
        if entry is None:
            logger.info(
                "[refresh_expiring_caches] Session %s has no active engine — skipping refresh",
                session_id,
            )
            skipped += 1
            continue

        logger.info("[refresh_expiring_caches] Refreshing cache for session %s", session_id)
        warm_cache.apply_async(
            args=[session_id, entry.connection_string],
            queue="schema-celery",
        )
        refreshed += 1

    logger.info(
        "[refresh_expiring_caches] Done — refreshed=%d, skipped=%d",
        refreshed,
        skipped,
    )
    return {"refreshed": refreshed, "skipped": skipped}
