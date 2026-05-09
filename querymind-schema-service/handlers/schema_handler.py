# querymind-schema-service/handlers/schema_handler.py
"""
Message handlers for the Schema Service.

Each handler is called by the RabbitMQ consumer dispatcher in main.py.
Handlers perform the domain logic and return the appropriate reply model.
All reply publishing is done by the consumer dispatcher (not here).
"""

import logging
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ArgumentError

from querymind_shared.schemas import (
    SchemaConnectRequest,
    SchemaConnectReply,
    SchemaGetRequest,
    SchemaGetReply,
    SchemaGetTablesRequest,
    SchemaGetTablesReply,
    SchemaRefreshRequest,
    SchemaRefreshReply,
    SchemaDisconnectRequest,
    SchemaDisconnectReply,
)

from core.config import settings
from core.session_store import session_store
from services.cache import (
    get_cached_tables,
    get_index,
    get_ttl_remaining,
    invalidate_session,
    store_schema_chunks,
)
from services.introspector import (
    introspect_database,
    get_table_list_with_estimates,
)

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _reply_base(request_msg) -> dict:
    """Build the common fields for all reply messages."""
    return {
        "correlation_id": request_msg.correlation_id,
        "session_id": request_msg.session_id,
        "timestamp": _now_iso(),
    }


# ---------------------------------------------------------------------------
# handle_connect
# ---------------------------------------------------------------------------

def handle_connect(payload: SchemaConnectRequest) -> SchemaConnectReply:
    """
    Validate and establish a connection to the user's database.
    Immediately returns a reply; cache warming is handled asynchronously by Celery.
    """
    session_id = payload.session_id
    conn_str = payload.connection_string

    # If already connected, dispose the old engine first
    if session_store.has(session_id):
        logger.info("[connect] Session %s already exists — replacing engine", session_id)
        invalidate_session(session_id)
        session_store.remove(session_id)

    try:
        engine = create_engine(
            conn_str,
            pool_pre_ping=True,
            pool_size=settings.DB_POOL_SIZE,
            max_overflow=2,
            connect_args={"connect_timeout": settings.DB_POOL_TIMEOUT},
        )

        # Test connectivity
        with engine.connect() as conn:
            result = conn.execute(text("SELECT current_database()"))
            row = result.fetchone()
            database_name = row[0] if row else "unknown"

        entry = session_store.add(session_id, engine, database_name, conn_str)

        # Fire-and-forget: warm cache in background Celery worker
        from tasks.schema_tasks import warm_cache  # local import to avoid circular
        warm_cache.apply_async(
            args=[session_id, conn_str],
            queue="schema-celery",
        )
        logger.info("[connect] Session %s connected to '%s'; warm_cache dispatched", session_id, database_name)

        return SchemaConnectReply(
            **_reply_base(payload),
            success=True,
            database_name=database_name,
            connected_at=entry.connected_at,
            message=f"Connected to '{database_name}'. Schema warm-up in progress.",
        )

    except ArgumentError as exc:
        logger.error("[connect] Invalid connection string for session %s: %s", session_id, exc)
        return SchemaConnectReply(
            **_reply_base(payload),
            success=False,
            error=f"Invalid connection string: {exc}",
        )

    except OperationalError as exc:
        logger.error("[connect] DB connection failed for session %s: %s", session_id, exc)
        return SchemaConnectReply(
            **_reply_base(payload),
            success=False,
            error=f"Database connection failed: {exc.orig}",
        )

    except Exception as exc:
        logger.exception("[connect] Unexpected error for session %s", session_id)
        return SchemaConnectReply(
            **_reply_base(payload),
            success=False,
            error=f"Unexpected error: {exc}",
        )


# ---------------------------------------------------------------------------
# handle_get
# ---------------------------------------------------------------------------

def handle_get(payload: SchemaGetRequest) -> SchemaGetReply:
    """
    Return schema chunks from Redis cache.
    If relevant_tables is specified, return only those chunks; otherwise return all.
    Falls back to live introspection if cache is cold (warm_cache still running).
    """
    session_id = payload.session_id
    requested_tables: list[str] = payload.relevant_tables or []

    entry = session_store.get(session_id)
    if entry is None:
        return SchemaGetReply(
            **_reply_base(payload),
            success=False,
            error=f"No active session found for session_id={session_id}. Please connect first.",
        )

    # Try cache
    cached = get_cached_tables(session_id, requested_tables if requested_tables else None)
    ttl_remaining = get_ttl_remaining(session_id)

    if cached:
        logger.info(
            "[get] Cache hit for session %s — %d/%s tables",
            session_id,
            len(cached),
            len(requested_tables) if requested_tables else "all",
        )
        schema_dict = {
            "database_name": entry.database_name,
            "tables": list(cached.values()),
            "total_tables": len(cached),
        }
        partial = bool(requested_tables and len(cached) < len(requested_tables))
        return SchemaGetReply(
            **_reply_base(payload),
            success=True,
            database_name=entry.database_name,
            cached=True,
            cache_ttl_remaining_seconds=ttl_remaining,
            partial=partial,
            schema=schema_dict,
        )

    # Cache miss — introspect live
    logger.info("[get] Cache miss for session %s — introspecting live", session_id)
    try:
        db_schema = introspect_database(entry.engine, entry.database_name)

        # Store full schema in cache for next time
        store_schema_chunks(session_id, db_schema.tables, ttl=settings.SCHEMA_CACHE_TTL)

        if requested_tables:
            tables = [t for t in db_schema.tables if t.table_name in requested_tables]
            partial = len(tables) < len(requested_tables)
        else:
            tables = db_schema.tables
            partial = False

        schema_dict = {
            "database_name": entry.database_name,
            "tables": [t.model_dump() for t in tables],
            "total_tables": len(tables),
        }

        return SchemaGetReply(
            **_reply_base(payload),
            success=True,
            database_name=entry.database_name,
            cached=False,
            cache_ttl_remaining_seconds=-1,
            partial=partial,
            schema=schema_dict,
        )

    except Exception as exc:
        logger.exception("[get] Introspection failed for session %s", session_id)
        return SchemaGetReply(
            **_reply_base(payload),
            success=False,
            error=f"Schema introspection failed: {exc}",
        )


# ---------------------------------------------------------------------------
# handle_get_tables
# ---------------------------------------------------------------------------

def handle_get_tables(payload: SchemaGetTablesRequest) -> SchemaGetTablesReply:
    """
    Return a lightweight list of {table_name, row_count_estimate} for the session's database.
    Used by the AI Service for RAG table-relevance detection.
    """
    session_id = payload.session_id

    entry = session_store.get(session_id)
    if entry is None:
        return SchemaGetTablesReply(
            **_reply_base(payload),
            success=False,
            error=f"No active session for session_id={session_id}",
        )

    # Check if index is cached — extract table names + estimates from cached chunks if available
    index = get_index(session_id)
    if index:
        # Return from cache index
        tables = [{"table_name": t, "row_count_estimate": 0} for t in index]
        # Try to enrich with estimates from cached chunks
        cached = get_cached_tables(session_id, index)
        tables = [
            {
                "table_name": t,
                "row_count_estimate": cached.get(t, {}).get("row_count_estimate", 0),
            }
            for t in index
        ]
        logger.info("[get_tables] Returning %d tables from cache for session %s", len(tables), session_id)
        return SchemaGetTablesReply(**_reply_base(payload), success=True, tables=tables)

    # Live introspection fallback
    try:
        tables = get_table_list_with_estimates(entry.engine)
        logger.info("[get_tables] Live — returned %d tables for session %s", len(tables), session_id)
        return SchemaGetTablesReply(**_reply_base(payload), success=True, tables=tables)
    except Exception as exc:
        logger.exception("[get_tables] Failed for session %s", session_id)
        return SchemaGetTablesReply(
            **_reply_base(payload),
            success=False,
            error=f"Failed to list tables: {exc}",
        )


# ---------------------------------------------------------------------------
# handle_refresh
# ---------------------------------------------------------------------------

def handle_refresh(payload: SchemaRefreshRequest) -> SchemaRefreshReply:
    """
    Invalidate the Redis cache for this session and re-introspect immediately.
    Used when the user wants a forced schema refresh.
    """
    session_id = payload.session_id

    entry = session_store.get(session_id)
    if entry is None:
        return SchemaRefreshReply(
            **_reply_base(payload),
            success=False,
            error=f"No active session for session_id={session_id}",
        )

    # Wipe old cache
    invalidate_session(session_id)

    try:
        db_schema = introspect_database(entry.engine, entry.database_name)
        store_schema_chunks(session_id, db_schema.tables, ttl=settings.SCHEMA_CACHE_TTL)

        schema_dict = {
            "database_name": entry.database_name,
            "tables": [t.model_dump() for t in db_schema.tables],
            "total_tables": db_schema.total_tables,
            "introspected_at": db_schema.introspected_at,
        }

        logger.info("[refresh] Done for session %s — %d tables", session_id, db_schema.total_tables)
        return SchemaRefreshReply(**_reply_base(payload), success=True, schema=schema_dict)

    except Exception as exc:
        logger.exception("[refresh] Failed for session %s", session_id)
        return SchemaRefreshReply(
            **_reply_base(payload),
            success=False,
            error=f"Schema refresh failed: {exc}",
        )


# ---------------------------------------------------------------------------
# handle_disconnect
# ---------------------------------------------------------------------------

def handle_disconnect(payload: SchemaDisconnectRequest) -> SchemaDisconnectReply:
    """
    Close the database engine and remove all cached schema data for this session.
    """
    session_id = payload.session_id

    if not session_store.has(session_id):
        logger.warning("[disconnect] Session %s not found — no-op", session_id)
        return SchemaDisconnectReply(
            **_reply_base(payload),
            success=True,  # Idempotent — already gone
        )

    try:
        invalidate_session(session_id)
        session_store.remove(session_id)
        logger.info("[disconnect] Session %s disconnected and cache invalidated", session_id)
        return SchemaDisconnectReply(**_reply_base(payload), success=True)

    except Exception as exc:
        logger.exception("[disconnect] Error disconnecting session %s", session_id)
        return SchemaDisconnectReply(
            **_reply_base(payload),
            success=False,
            error=f"Disconnect error: {exc}",
        )
