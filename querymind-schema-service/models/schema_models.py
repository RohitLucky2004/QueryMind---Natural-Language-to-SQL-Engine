# querymind-schema-service/models/schema_models.py
"""
Internal Pydantic models for the Schema Service.
These are used for internal data representation, not as HTTP DTOs.
"""

from typing import Any
from pydantic import BaseModel


class ColumnInfo(BaseModel):
    name: str
    type: str
    nullable: bool
    primary_key: bool
    default: str | None = None
    comment: str | None = None


class ForeignKeyInfo(BaseModel):
    constrained_columns: list[str]
    referred_table: str
    referred_columns: list[str]


class IndexInfo(BaseModel):
    name: str
    columns: list[str]
    unique: bool


class TableInfo(BaseModel):
    table_name: str
    schema: str = "public"
    columns: list[ColumnInfo] = []
    primary_keys: list[str] = []
    foreign_keys: list[ForeignKeyInfo] = []
    indexes: list[IndexInfo] = []
    row_count_estimate: int = 0
    comment: str | None = None


class DatabaseSchema(BaseModel):
    database_name: str
    tables: list[TableInfo] = []
    total_tables: int = 0
    introspected_at: str


class SessionInfo(BaseModel):
    session_id: str
    connection_string: str
    database_name: str
    connected_at: str


class CacheChunk(BaseModel):
    table_name: str
    session_id: str
    schema_json: dict[str, Any]
    cached_at: str
    ttl_seconds: int = 3600
