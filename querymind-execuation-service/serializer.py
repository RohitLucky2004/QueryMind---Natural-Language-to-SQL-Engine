import logging
from decimal import Decimal
from datetime import datetime, date, time, timedelta
from uuid import UUID
from typing import Any

logger = logging.getLogger(__name__)


def to_json_safe(value: Any) -> Any:
    """
    Recursively convert a value to a JSON-serialisable type.

    Handles the PostgreSQL/SQLAlchemy types that the standard json module
    cannot serialise: Decimal, datetime, date, time, timedelta, UUID, bytes,
    and unknown objects (falls back to str()).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, str)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (list, tuple)):
        return [to_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: to_json_safe(v) for k, v in value.items()}
    # Fallback — unknown type, convert to string
    logger.debug("Unrecognised type %s — converting to str", type(value).__name__)
    return str(value)


def serialise_rows(
    raw_rows: list,
    column_names: list[str],
) -> tuple[list[dict], list[list]]:
    """
    Convert SQLAlchemy result rows into:
    - columns: list of {"name": col_name} dicts for the frontend table header
    - rows: list of JSON-safe value arrays

    Args:
        raw_rows: list of SQLAlchemy Row objects.
        column_names: Ordered list of column name strings.

    Returns:
        (columns, rows)
    """
    columns = [{"name": name} for name in column_names]
    rows = [
        [to_json_safe(cell) for cell in row]
        for row in raw_rows
    ]
    return columns, rows
