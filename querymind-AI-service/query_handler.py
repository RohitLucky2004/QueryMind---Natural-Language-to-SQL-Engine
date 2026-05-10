import hashlib
import json
import logging
import time
from datetime import datetime, timezone

from querymind_shared.schemas import AIQueryGenerateRequest, AIQueryGenerateReply
from querymind_shared.publisher import Publisher
from core.config import settings
from core.redis_client import cache_get, cache_set
from services.rag_retriever import retrieve_relevant_schema
from services.ai_generator import generate_sql
from tasks.ai_tasks import cache_result, log_usage

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_cache_key(session_id: str, question: str) -> str:
    """SHA256-based cache key scoped to session to prevent cross-DB pollution."""
    raw = f"{session_id}:{question}"
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return f"query_cache:{digest}"


def handle_generate(
    payload: AIQueryGenerateRequest,
    reply_to: str,
    publisher: Publisher,
) -> None:
    """
    Main handler for ai.query.generate.request messages.

    Pipeline:
    1. Check Redis query cache — return immediately on hit.
    2. Retrieve relevant schema via two-step RAG RPC to Schema Service.
    3. Call Claude to generate SQL.
    4. Run three-pass SQL validation (safety → AST → injection).
    5. Publish AIQueryGenerateReply to reply_to queue.
    6. Fire Celery tasks for caching and usage logging (fire-and-forget).

    Args:
        payload: Validated AIQueryGenerateRequest message.
        reply_to: RabbitMQ reply queue (amq.rabbitmq.reply-to).
        publisher: RabbitMQ publisher for sending the reply.
    """
    session_id = payload.session_id
    question = payload.question
    correlation_id = payload.correlation_id

    logger.info(
        "[handle_generate] session=%s question=%.100s", session_id, question
    )

    # ── 1. Query cache check ────────────────────────────────────────────────
    cache_key = _make_cache_key(session_id, question)
    cached = cache_get(cache_key)
    if cached:
        try:
            cached_result = json.loads(cached)
            logger.info("[handle_generate] Cache HIT for session=%s", session_id)

            reply = AIQueryGenerateReply(
                correlation_id=correlation_id,
                session_id=session_id,
                timestamp=_now_iso(),
                success=True,
                sql=cached_result.get("sql"),
                rationale=cached_result.get("rationale", ""),
                explanation=cached_result.get("explanation", ""),
                tables_used=cached_result.get("tables_used", []),
                cache_hit=True,
                generation_time_ms=0,
            )
            publisher.publish_to_reply_queue(reply_to=reply_to, payload=reply)

            # Still log usage even on cache hit
            log_usage.delay(session_id, question, 0, 0)
            return
        except Exception as e:
            logger.warning("[handle_generate] Cache parse error, regenerating: %s", e)

    # ── 2. RAG schema retrieval ─────────────────────────────────────────────
    start_time = time.monotonic()
    try:
        schema, rag_context = retrieve_relevant_schema(session_id, question)
    except (TimeoutError, RuntimeError) as e:
        logger.error("[handle_generate] RAG retrieval failed: %s", e)
        _publish_error(
            publisher, reply_to, correlation_id, session_id,
            error=str(e), error_type="schema_retrieval_error"
        )
        return

    # ── 3 & 4. Claude generation + validation ──────────────────────────────
    try:
        result = generate_sql(question=question, schema=schema)
    except ValueError as e:
        logger.warning("[handle_generate] Validation/generation failed: %s", e)
        _publish_error(
            publisher, reply_to, correlation_id, session_id,
            error=str(e), error_type="validation_error"
        )
        return
    except Exception as e:
        logger.error("[handle_generate] Unexpected generation error: %s", e)
        _publish_error(
            publisher, reply_to, correlation_id, session_id,
            error=str(e), error_type="generation_error"
        )
        return

    total_latency_ms = int((time.monotonic() - start_time) * 1000)

    # ── 5. Publish reply ────────────────────────────────────────────────────
    reply = AIQueryGenerateReply(
        correlation_id=correlation_id,
        session_id=session_id,
        timestamp=_now_iso(),
        success=True,
        sql=result.sql,
        rationale=result.rationale,
        explanation=result.explanation,
        tables_used=result.tables_used,
        validation=result.validation,
        generation_time_ms=total_latency_ms,
        cache_hit=False,
        rag_context=rag_context,
    )
    publisher.publish_to_reply_queue(reply_to=reply_to, payload=reply)
    logger.info(
        "[handle_generate] Reply published — session=%s latency=%dms tokens=%d",
        session_id,
        total_latency_ms,
        result.tokens_used,
    )

    # ── 6. Fire-and-forget Celery tasks ────────────────────────────────────
    result_json = json.dumps({
        "sql": result.sql,
        "rationale": result.rationale,
        "explanation": result.explanation,
        "tables_used": result.tables_used,
    })
    cache_result.delay(cache_key, result_json)
    log_usage.delay(session_id, question, result.tokens_used, result.latency_ms)


def _publish_error(
    publisher: Publisher,
    reply_to: str,
    correlation_id: str,
    session_id: str,
    error: str,
    error_type: str,
) -> None:
    """Helper to publish an error AIQueryGenerateReply."""
    reply = AIQueryGenerateReply(
        correlation_id=correlation_id,
        session_id=session_id,
        timestamp=_now_iso(),
        success=False,
        error=error,
        error_type=error_type,
    )
    publisher.publish_to_reply_queue(reply_to=reply_to, payload=reply)
