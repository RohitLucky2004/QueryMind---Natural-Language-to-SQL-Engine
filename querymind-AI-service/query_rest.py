import logging
from uuid import uuid4
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services.rag_retriever import retrieve_relevant_schema
from services.ai_generator import generate_sql
from tasks.ai_tasks import cache_result, log_usage
from core.redis_client import cache_get
from core.rpc import get_rpc_client, reset_rpc_client
from querymind_shared.events import SCHEMA_GET_TABLES_REQUEST
from querymind_shared.schemas import SchemaGetTablesRequest
import hashlib
import json

router = APIRouter()
logger = logging.getLogger(__name__)

_consumer_thread_ref = None  # Set by main.py after thread start


def set_consumer_thread(thread):
    global _consumer_thread_ref
    _consumer_thread_ref = thread


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_cache_key(session_id: str, question: str) -> str:
    digest = hashlib.sha256(f"{session_id}:{question}".encode()).hexdigest()
    return f"query_cache:{digest}"


class GenerateRequest(BaseModel):
    session_id: str
    question: str


class GenerateResponse(BaseModel):
    success: bool
    sql: str | None = None
    rationale: str | None = None
    explanation: str | None = None
    tables_used: list[str] = []
    cache_hit: bool = False
    generation_time_ms: int | None = None
    error: str | None = None
    error_type: str | None = None


@router.post("/query/generate", response_model=GenerateResponse)
async def generate_query(request: GenerateRequest):
    """
    Non-streaming REST fallback for SQL generation.
    Returns the full validated SQL result in a single response.
    """
    session_id = request.session_id
    question = request.question.strip()

    if not question:
        raise HTTPException(status_code=400, detail="Question must not be empty")

    # Cache check
    cache_key = _make_cache_key(session_id, question)
    cached = cache_get(cache_key)
    if cached:
        try:
            cached_result = json.loads(cached)
            log_usage.delay(session_id, question, 0, 0)
            return GenerateResponse(
                success=True,
                cache_hit=True,
                **{k: cached_result.get(k) for k in ("sql", "rationale", "explanation", "tables_used")},
            )
        except Exception:
            pass

    # RAG + generation
    try:
        schema, _ = retrieve_relevant_schema(session_id, question)
        result = generate_sql(question=question, schema=schema)
    except (TimeoutError, RuntimeError) as e:
        return GenerateResponse(success=False, error=str(e), error_type="schema_retrieval_error")
    except ValueError as e:
        return GenerateResponse(success=False, error=str(e), error_type="validation_error")
    except Exception as e:
        logger.error("REST generate unexpected error: %s", e)
        return GenerateResponse(success=False, error=str(e), error_type="internal_error")

    # Fire-and-forget caching
    result_json = json.dumps({
        "sql": result.sql,
        "rationale": result.rationale,
        "explanation": result.explanation,
        "tables_used": result.tables_used,
    })
    cache_result.delay(cache_key, result_json)
    log_usage.delay(session_id, question, result.tokens_used, result.latency_ms)

    return GenerateResponse(
        success=True,
        sql=result.sql,
        rationale=result.rationale,
        explanation=result.explanation,
        tables_used=result.tables_used,
        generation_time_ms=result.latency_ms,
    )


@router.get("/health")
async def health():
    """
    Health check endpoint.
    Tests RabbitMQ consumer liveness and Schema Service reachability.
    """
    rabbitmq_status = "connected"
    schema_reachable = False

    # Check consumer thread liveness
    if _consumer_thread_ref is not None and not _consumer_thread_ref.is_alive():
        rabbitmq_status = "disconnected"

    # Probe Schema Service via a dummy RPC call
    try:
        rpc = get_rpc_client()
        reply = rpc.call(
            routing_key=SCHEMA_GET_TABLES_REQUEST,
            payload=SchemaGetTablesRequest(
                correlation_id=str(uuid4()),
                session_id="health-check-probe",
                timestamp=_now_iso(),
            ),
            timeout=5.0,
        )
        # Any reply (even error) means Schema Service is alive and MQ is working
        schema_reachable = True
    except TimeoutError:
        schema_reachable = False
        reset_rpc_client()
    except Exception as e:
        logger.warning("Health check RPC error: %s", e)
        schema_reachable = False
        reset_rpc_client()

    return {
        "service": "ai-query-service",
        "status": "ok",
        "port": 8002,
        "rabbitmq": rabbitmq_status,
        "celery": "ok",
        "schema_service_reachable": schema_reachable,
    }
