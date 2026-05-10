import re
import logging

logger = logging.getLogger(__name__)

# Identical forbidden keyword set as AI Service Pass 1 — defense in depth
FORBIDDEN_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|REPLACE|MERGE"
    r"|GRANT|REVOKE|EXEC|EXECUTE|CALL|COPY|VACUUM|ANALYZE|REINDEX"
    r"|LOCK|CLUSTER|COMMENT|SECURITY|OWNER|RENAME)\b",
    re.IGNORECASE,
)


def check_operation_safety(sql: str) -> tuple[bool, str | None]:
    """
    Re-run Pass 1 safety regex immediately before execution.

    Defense in depth: even if the AI Service validated the SQL, the Execution
    Service applies its own check before touching any database. This means
    no single validation layer is trusted alone.

    Args:
        sql: The SQL to execute.

    Returns:
        (passed, reason) — passed=True means safe to execute.
    """
    match = FORBIDDEN_PATTERN.search(sql)
    if match:
        keyword = match.group(0).upper()
        reason = f"Execution blocked — forbidden SQL operation: {keyword}"
        logger.warning("Execution safety check FAILED — %s found in SQL: %.200s", keyword, sql)
        return False, reason

    logger.debug("Execution safety check passed")
    return True, None
