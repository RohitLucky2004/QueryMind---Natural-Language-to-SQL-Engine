import hashlib
import json
import logging
from datetime import datetime, timezone

from querymind_shared.schemas import (
    ExecInitRequest,
    ExecInitReply,
    ExecRunRequest,
    ExecRunReply,
    ExecHistoryRequest,
    ExecHistoryReply,
)
from querymind_shared.publisher import Publisher

from core.session_store import create_session_engine, get_session_engine
from core.redis_client import get_redis
from core.config import settings
from services.executor import execute_query
from tasks.exec_tasks import persist_history, archive_result

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sql_hash(sql: str) -> str:
    return hashlib.sha256(sql.encode()).hexdigest()[:16]


# ── handle_init ─────────────────────────────────────────────────────────────

def handle_init(
    payload: ExecInitRequest,
    reply_to: str,
    publisher: Publisher,
) -> None:
    """
    Handle exec.init.request — register the user's database session.

    Creates a SQLAlchemy engine for the provided connection_string and stores
    it in the in-memory session store keyed by session_id.
    The connection_string is passed directly in the message from the Gateway —
    the Execution Service never calls Schema Service.

    Args:
        payload: ExecInitRequest with session_id and connection_string.
        reply_to: RabbitMQ reply queue.
        publisher: Publisher for sending ExecInitReply.
    """
    session_id = payload.session_id
    logger.info("[handle_init] Initialising session=%s", session_id)

    try:
        create_session_engine(session_id, payload.connection_string)
        reply = ExecInitReply(
            correlation_id=payload.correlation_id,
            session_id=session_id,
            timestamp=_now_iso(),
            success=True,
        )
        logger.info("[handle_init] Session initialised — session=%s", session_id)
    except Exception as e:
        logger.error("[handle_init] Failed to init session=%s: %s", session_id, e)
        reply = ExecInitReply(
            correlation_id=payload.correlation_id,
            session_id=session_id,
            timestamp=_now_iso(),
            success=False,
            error=str(e),
        )

    publisher.publish_to_reply_queue(reply_to=reply_to, payload=reply)


# ── handle_run ──────────────────────────────────────────────────────────────

def handle_run(
    payload: ExecRunRequest,
    reply_to: str,
    publisher: Publisher,
) -> None:
    """
    Handle exec.run.request — execute a validated SQL query.

    Pipeline:
    1. Look up the session engine.
    2. Execute SQL with READ ONLY transaction + statement_timeout.
    3. Serialise and paginate results.
    4. Publish ExecRunReply.
    5. Fire Celery tasks: persist_history + archive_result (fire-and-forget).

    Args:
        payload: ExecRunRequest with session_id, sql, page, page_size.
        reply_to: RabbitMQ reply queue.
        publisher: Publisher for sending ExecRunReply.
    """
    session_id = payload.session_id
    sql = payload.sql.strip()
    page = payload.page or 1
    page_size = payload.page_size or 50

    logger.info(
        "[handle_run] session=%s page=%d sql=%.100s",
        session_id, page, sql,
    )

    engine = get_session_engine(session_id)
    if engine is None:
        logger.warning("[handle_run] No active session for session_id=%s", session_id)
        reply = ExecRunReply(
            correlation_id=payload.correlation_id,
            session_id=session_id,
            timestamp=_now_iso(),
            success=False,
            error="No active session found. Please reconnect your database.",
            error_type="session_not_found",
        )
        publisher.publish_to_reply_queue(reply_to=reply_to, payload=reply)
        return

    try:
        result = execute_query(engine=engine, sql=sql, page=page, page_size=page_size)

        reply = ExecRunReply(
            correlation_id=payload.correlation_id,
            session_id=session_id,
            timestamp=_now_iso(),
            success=True,
            sql_executed=result.sql_executed,
            columns=result.columns,
            rows=result.rows,
            pagination=result.pagination,
            execution_time_ms=result.execution_time_ms,
            truncated=result.truncated,
            truncation_warning=result.truncation_warning,
        )
        publisher.publish_to_reply_queue(reply_to=reply_to, payload=reply)

        logger.info(
            "[handle_run] Execution complete — session=%s rows=%d time=%dms",
            session_id, result.row_count, result.execution_time_ms,
        )

        # ── Fire-and-forget Celery tasks ──────────────────────────────────
        history_record = {
            "sql": sql,
            "executed_at": _now_iso(),
            "execution_time_ms": result.execution_time_ms,
            "row_count": result.row_count,
            "success": True,
            "error": None,
        }
        persist_history.delay(session_id, history_record)

        result_json = json.dumps({
            "columns": result.columns,
            "rows": result.rows,
            "pagination": result.pagination,
            "truncated": result.truncated,
        })
        archive_result.delay(session_id, _sql_hash(sql), result_json)

    except ValueError as e:
        # Safety check failure
        logger.warning("[handle_run] Safety check failed session=%s: %s", session_id, e)
        _publish_run_error(publisher, reply_to, payload, str(e), "safety_error")
        _record_failed_history(session_id, sql, str(e))

    except Exception as e:
        logger.error("[handle_run] Execution error session=%s: %s", session_id, e, exc_info=True)
        error_msg = str(e)

        # Classify common PostgreSQL errors
        error_type = "execution_error"
        err_lower = error_msg.lower()
        if "timeout" in err_lower or "statement_timeout" in err_lower:
            error_type = "timeout_error"
        elif "permission" in err_lower or "denied" in err_lower:
            error_type = "permission_error"
        elif "syntax" in err_lower:
            error_type = "syntax_error"
        elif "does not exist" in err_lower:
            error_type = "reference_error"

        _publish_run_error(publisher, reply_to, payload, error_msg, error_type)
        _record_failed_history(session_id, sql, error_msg)


def _publish_run_error(
    publisher: Publisher,
    reply_to: str,
    payload: ExecRunRequest,
    error: str,
    error_type: str,
) -> None:
    reply = ExecRunReply(
        correlation_id=payload.correlation_id,
        session_id=payload.session_id,
        timestamp=_now_iso(),
        success=False,
        error=error,
        error_type=error_type,
    )
    publisher.publish_to_reply_queue(reply_to=reply_to, payload=reply)


def _record_failed_history(session_id: str, sql: str, error: str) -> None:
    """Persist a failed execution record to history (fire-and-forget)."""
    record = {
        "sql": sql,
        "executed_at": _now_iso(),
        "execution_time_ms": 0,
        "row_count": 0,
        "success": False,
        "error": error,
    }
    persist_history.delay(session_id, record)


# ── handle_history ──────────────────────────────────────────────────────────

def handle_history(
    payload: ExecHistoryRequest,
    reply_to: str,
    publisher: Publisher,
) -> None:
    """
    Handle exec.history.request — return the last N executed queries for a session.

    Reads from Redis exec_history:{session_id} list — history survives service restarts.

    Args:
        payload: ExecHistoryRequest with session_id.
        reply_to: RabbitMQ reply queue.
        publisher: Publisher for sending ExecHistoryReply.
    """
    session_id = payload.session_id
    logger.info("[handle_history] session=%s", session_id)

    try:
        r = get_redis()
        key = f"exec_history:{session_id}"
        raw_entries = r.lrange(key, 0, 19)  # last 20 entries

        history = []
        for raw in raw_entries:
            try:
                history.append(json.loads(raw))
            except json.JSONDecodeError:
                continue

        reply = ExecHistoryReply(
            correlation_id=payload.correlation_id,
            session_id=session_id,
            timestamp=_now_iso(),
            success=True,
            history=history,
        )
        logger.info("[handle_history] Returning %d history entries for session=%s", len(history), session_id)

    except Exception as e:
        logger.error("[handle_history] Error for session=%s: %s", session_id, e)
        reply = ExecHistoryReply(
            correlation_id=payload.correlation_id,
            session_id=session_id,
            timestamp=_now_iso(),
            success=False,
            error=str(e),
        )

    publisher.publish_to_reply_queue(reply_to=reply_to, payload=reply)
