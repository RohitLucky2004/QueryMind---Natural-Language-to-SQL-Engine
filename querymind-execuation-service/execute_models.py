from pydantic import BaseModel
from typing import Any


class ExecutionResult(BaseModel):
    """Result of a successful SQL execution."""
    sql_executed: str
    columns: list[dict]          # [{"name": ..., "type": ...}]
    rows: list[list]             # list of row value arrays
    row_count: int
    execution_time_ms: int
    truncated: bool = False
    truncation_warning: str | None = None
    pagination: dict | None = None


class HistoryRecord(BaseModel):
    """A single entry in execution history."""
    sql: str
    executed_at: str             # ISO datetime
    execution_time_ms: int
    row_count: int
    success: bool
    error: str | None = None
