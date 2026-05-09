# querymind-shared/querymind_shared/schemas.py
"""
Pydantic message schemas for every event published or consumed between QueryMind services.
All models inherit from BaseMessage which carries the correlation envelope.
"""

from pydantic import BaseModel, Field


class BaseMessage(BaseModel):
    correlation_id: str    # UUID — used to match replies to requests
    session_id: str
    timestamp: str         # ISO 8601 datetime string


# ── Schema Service messages ────────────────────────────────────────────────

class SchemaConnectRequest(BaseMessage):
    connection_string: str


class SchemaConnectReply(BaseMessage):
    success: bool
    database_name: str | None = None
    connected_at: str | None = None
    error: str | None = None
    message: str | None = None


class SchemaGetRequest(BaseMessage):
    relevant_tables: list[str] = Field(default_factory=list)  # empty = fetch all


class SchemaGetReply(BaseMessage):
    success: bool
    database_name: str | None = None
    cached: bool = False
    cache_ttl_remaining_seconds: int = -1
    partial: bool = False
    schema: dict | None = None
    error: str | None = None
    message: str | None = None


class SchemaGetTablesRequest(BaseMessage):
    pass


class SchemaGetTablesReply(BaseMessage):
    success: bool
    tables: list[dict] = Field(default_factory=list)  # [{table_name, row_count_estimate}]
    error: str | None = None


class SchemaRefreshRequest(BaseMessage):
    pass


class SchemaRefreshReply(BaseMessage):
    success: bool
    schema: dict | None = None
    error: str | None = None


class SchemaDisconnectRequest(BaseMessage):
    pass


class SchemaDisconnectReply(BaseMessage):
    success: bool
    error: str | None = None


# ── AI Service messages ────────────────────────────────────────────────────

class AIQueryGenerateRequest(BaseMessage):
    question: str
    # Note: AI service fetches schema itself via RabbitMQ — no schema passed here


class AIQueryGenerateReply(BaseMessage):
    success: bool
    sql: str | None = None
    rationale: str | None = None
    explanation: str | None = None
    tables_used: list[str] = Field(default_factory=list)
    validation: dict | None = None
    generation_time_ms: int | None = None
    cache_hit: bool = False
    rag_context: dict | None = None
    error: str | None = None
    error_type: str | None = None
    invalid_references: list[str] = Field(default_factory=list)


# ── Execution Service messages ─────────────────────────────────────────────

class ExecInitRequest(BaseMessage):
    connection_string: str


class ExecInitReply(BaseMessage):
    success: bool
    error: str | None = None


class ExecRunRequest(BaseMessage):
    sql: str
    page: int = 1
    page_size: int = 50


class ExecRunReply(BaseMessage):
    success: bool
    sql_executed: str | None = None
    columns: list[dict] = Field(default_factory=list)
    rows: list[list] = Field(default_factory=list)
    pagination: dict | None = None
    execution_time_ms: int | None = None
    truncated: bool = False
    truncation_warning: str | None = None
    error: str | None = None
    error_type: str | None = None


class ExecHistoryRequest(BaseMessage):
    pass


class ExecHistoryReply(BaseMessage):
    success: bool
    history: list[dict] = Field(default_factory=list)
    error: str | None = None
