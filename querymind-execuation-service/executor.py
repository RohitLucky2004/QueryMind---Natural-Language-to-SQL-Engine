import hashlib
import logging
import time
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.config import settings
from models.execute_models import ExecutionResult
from services.safety import check_operation_safety
from services.serializer import serialise_rows

logger = logging.getLogger(__name__)


def _make_sql_hash(sql: str) -> str:
    return hashlib.sha256(sql.encode()).hexdigest()[:16]


def execute_query(
    engine: Engine,
    sql: str,
    page: int = 1,
    page_size: int = 50,
) -> ExecutionResult:
    """
    Execute a validated SQL query safely against the user's database.

    Safety guarantees applied at the database level:
    1. Re-runs operation safety regex (defense in depth).
    2. SET TRANSACTION READ ONLY — physically prevents writes at the DB level.
    3. SET LOCAL statement_timeout — kills runaway queries after N ms.
    4. Truncates result sets at MAX_RESULT_ROWS to prevent memory exhaustion.
    5. Paginates the returned rows for the frontend.

    Args:
        engine: SQLAlchemy engine for the user's database.
        sql: Validated SQL to execute.
        page: 1-indexed page number.
        page_size: Number of rows per page.

    Returns:
        ExecutionResult with serialised rows and pagination metadata.

    Raises:
        ValueError: If the SQL fails the safety check.
        Exception: On database-level errors (propagated to handler).
    """
    # Defense-in-depth safety check before touching the database
    passed, reason = check_operation_safety(sql)
    if not passed:
        raise ValueError(reason)

    page = max(1, page)
    page_size = max(1, min(page_size, 500))  # cap page_size at 500

    start_time = time.monotonic()

    with engine.connect() as conn:
        # ── PostgreSQL transaction-level safety ──────────────────────────
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(
            text(f"SET LOCAL statement_timeout = '{settings.STATEMENT_TIMEOUT_MS}'")
        )

        logger.info("Executing SQL: %.200s", sql.strip())

        result_proxy = conn.execute(text(sql))
        column_names = list(result_proxy.keys())

        # Fetch up to MAX_RESULT_ROWS + 1 to detect truncation
        fetch_limit = settings.MAX_RESULT_ROWS + 1
        raw_rows = result_proxy.fetchmany(fetch_limit)

        truncated = len(raw_rows) > settings.MAX_RESULT_ROWS
        if truncated:
            raw_rows = raw_rows[:settings.MAX_RESULT_ROWS]
            truncation_warning = (
                f"Result set exceeds {settings.MAX_RESULT_ROWS} rows. "
                f"Showing first {settings.MAX_RESULT_ROWS} rows only."
            )
        else:
            truncation_warning = None

    execution_time_ms = int((time.monotonic() - start_time) * 1000)
    total_rows = len(raw_rows)

    logger.info(
        "Execution complete — rows=%d truncated=%s time=%dms",
        total_rows, truncated, execution_time_ms,
    )

    # Pagination over the already-fetched result set
    total_pages = max(1, -(-total_rows // page_size))  # ceiling division
    offset = (page - 1) * page_size
    page_rows = raw_rows[offset: offset + page_size]

    columns, serialised_rows = serialise_rows(page_rows, column_names)

    pagination = {
        "page": page,
        "page_size": page_size,
        "total_rows": total_rows,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1,
    }

    return ExecutionResult(
        sql_executed=sql,
        columns=columns,
        rows=serialised_rows,
        row_count=total_rows,
        execution_time_ms=execution_time_ms,
        truncated=truncated,
        truncation_warning=truncation_warning,
        pagination=pagination,
    )
