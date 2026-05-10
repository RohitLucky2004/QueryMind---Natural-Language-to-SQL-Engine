import re
import logging

logger = logging.getLogger(__name__)

# Forbidden SQL keywords — write/DDL operations that must never be executed
FORBIDDEN_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|REPLACE|MERGE"
    r"|GRANT|REVOKE|EXEC|EXECUTE|CALL|COPY|VACUUM|ANALYZE|REINDEX"
    r"|LOCK|CLUSTER|COMMENT|SECURITY|OWNER|RENAME)\b",
    re.IGNORECASE,
)


def check_operation_safety(sql: str) -> tuple[bool, str | None]:
    """
    Pass 1: Scan generated SQL for forbidden write/DDL keywords using regex.

    This is intentionally the first and cheapest pass — if SQL contains DELETE
    or DROP, there is no point running the expensive AST parser.

    Args:
        sql: The generated SQL string.

    Returns:
        (passed, reason) — passed=True means safe, False means blocked.
    """
    match = FORBIDDEN_PATTERN.search(sql)
    if match:
        keyword = match.group(0).upper()
        reason = f"Forbidden SQL operation detected: {keyword}"
        logger.warning("Pass 1 FAILED — %s in SQL: %.200s", keyword, sql)
        return False, reason

    logger.debug("Pass 1 passed — no forbidden keywords detected")
    return True, None
