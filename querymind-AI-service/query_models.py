from pydantic import BaseModel
from typing import Any


class GenerationResult(BaseModel):
    sql: str
    rationale: str
    explanation: str
    tables_used: list[str] = []
    tokens_used: int = 0
    latency_ms: int = 0
    cache_hit: bool = False
    validation: dict[str, Any] | None = None
    rag_context: dict[str, Any] | None = None


class ValidationResult(BaseModel):
    passed: bool
    failed_pass: int | None = None  # 1, 2, or 3
    reason: str | None = None
    invalid_references: list[str] = []


class RAGContext(BaseModel):
    total_tables: int
    selected_tables: list[str]
    selection_method: str = "keyword_relevance"
