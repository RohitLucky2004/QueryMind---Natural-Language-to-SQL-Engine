import logging
import re
from uuid import uuid4
from datetime import datetime, timezone
from typing import Any

from querymind_shared.events import SCHEMA_GET_TABLES_REQUEST, SCHEMA_GET_REQUEST
from querymind_shared.schemas import (
    SchemaGetTablesRequest,
    SchemaGetTablesReply,
    SchemaGetRequest,
    SchemaGetReply,
)
from core.rpc import get_rpc_client, reset_rpc_client

logger = logging.getLogger(__name__)

# Maximum number of tables to inject into the prompt
MAX_RELEVANT_TABLES = 10
# Minimum relevance score to include a table
MIN_RELEVANCE_SCORE = 0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tokenise(text: str) -> set[str]:
    """Lowercase word tokens from a string."""
    return set(re.findall(r"[a-z0-9_]+", text.lower()))


def _score_table(question_tokens: set[str], table_name: str, row_count: int) -> float:
    """
    Compute a relevance score for a table given the question tokens.

    Scoring:
    - +2 per exact token match against table name parts
    - +0.1 log-scale boost for larger tables (more important tables tend to be bigger)
    """
    import math

    table_tokens = _tokenise(table_name)
    overlap = len(question_tokens & table_tokens)
    size_boost = math.log10(max(row_count, 1)) * 0.1
    return overlap * 2.0 + size_boost


def retrieve_relevant_schema(
    session_id: str,
    question: str,
    max_tables: int = MAX_RELEVANT_TABLES,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Two-step RAG pipeline to retrieve only the relevant schema chunks.

    Step A: Fetch lightweight [{table_name, row_count_estimate}] list.
    Step B: Score relevance, select top-N, fetch full schema chunks.

    Args:
        session_id: The user's session identifier.
        question: The natural language question.
        max_tables: Maximum number of tables to retrieve full schema for.

    Returns:
        (schema_dict, rag_context_dict)

    Raises:
        TimeoutError: If Schema Service does not respond in time.
        RuntimeError: If Schema Service returns an error.
    """
    rpc = get_rpc_client()
    question_tokens = _tokenise(question)

    # ── Step A: Get lightweight table list ─────────────────────────────────
    logger.info("[RAG] Step A — fetching table list for session %s", session_id)
    try:
        raw_reply = rpc.call(
            routing_key=SCHEMA_GET_TABLES_REQUEST,
            payload=SchemaGetTablesRequest(
                correlation_id=str(uuid4()),
                session_id=session_id,
                timestamp=_now_iso(),
            ),
            timeout=10.0,
        )
    except TimeoutError:
        reset_rpc_client()
        raise TimeoutError("Schema Service did not respond to get_tables RPC within 10s")
    except Exception as e:
        reset_rpc_client()
        raise RuntimeError(f"RPC error during get_tables: {e}") from e

    tables_reply = SchemaGetTablesReply(**raw_reply)
    if not tables_reply.success:
        raise RuntimeError(
            f"Schema Service returned error for get_tables: {tables_reply.error}"
        )

    all_tables = tables_reply.tables  # [{table_name, row_count_estimate}]
    logger.info("[RAG] Step A — received %d tables", len(all_tables))

    # ── Relevance scoring ──────────────────────────────────────────────────
    scored = [
        (
            t["table_name"],
            _score_table(question_tokens, t["table_name"], t.get("row_count_estimate", 0)),
        )
        for t in all_tables
    ]
    scored.sort(key=lambda x: x[1], reverse=True)

    # If no tokens matched anything, fall back to top-N by row count (largest tables)
    if all(score == 0 for _, score in scored):
        logger.info("[RAG] No keyword matches — falling back to top-%d by row count", max_tables)
        selected_tables = [t["table_name"] for t in all_tables[:max_tables]]
    else:
        selected_tables = [
            name for name, score in scored[:max_tables] if score > MIN_RELEVANCE_SCORE
        ]

    logger.info("[RAG] Selected tables: %s", selected_tables)

    # ── Step B: Fetch full schema chunks for selected tables ───────────────
    logger.info("[RAG] Step B — fetching full schema for %d tables", len(selected_tables))
    try:
        raw_reply = rpc.call(
            routing_key=SCHEMA_GET_REQUEST,
            payload=SchemaGetRequest(
                correlation_id=str(uuid4()),
                session_id=session_id,
                relevant_tables=selected_tables,
                timestamp=_now_iso(),
            ),
            timeout=15.0,
        )
    except TimeoutError:
        reset_rpc_client()
        raise TimeoutError("Schema Service did not respond to get RPC within 15s")
    except Exception as e:
        reset_rpc_client()
        raise RuntimeError(f"RPC error during schema get: {e}") from e

    schema_reply = SchemaGetReply(**raw_reply)
    if not schema_reply.success:
        raise RuntimeError(
            f"Schema Service returned error for get: {schema_reply.error}"
        )

    schema = schema_reply.schema or {}
    logger.info("[RAG] Step B — received schema for %d tables", len(schema))

    rag_context = {
        "total_tables": len(all_tables),
        "selected_tables": selected_tables,
        "selection_method": "keyword_relevance",
        "cached": schema_reply.cached,
    }

    return schema, rag_context
