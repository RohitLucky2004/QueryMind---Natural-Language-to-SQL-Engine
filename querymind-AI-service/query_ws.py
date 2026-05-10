import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from services.rag_retriever import retrieve_relevant_schema
from services.ai_generator import stream_sql_tokens
from tasks.ai_tasks import cache_result, log_usage
from core.redis_client import cache_get, cache_set
import hashlib

router = APIRouter()
logger = logging.getLogger(__name__)


def _make_cache_key(session_id: str, question: str) -> str:
    digest = hashlib.sha256(f"{session_id}:{question}".encode()).hexdigest()
    return f"query_cache:{digest}"


@router.websocket("/ws/query/{session_id}")
async def websocket_query(websocket: WebSocket, session_id: str):
    """
    WebSocket endpoint for streaming SQL generation.

    The API Gateway proxies frontend WebSocket connections to this endpoint.
    Tokens are streamed as they arrive from Claude. A terminal '__RESULT__:...'
    frame is sent at the end with the complete validated result.

    NOTE: Do NOT publish to RabbitMQ from within this handler.
    Streaming is incompatible with RabbitMQ's message-at-a-time semantics.
    """
    await websocket.accept()
    logger.info("[WS] Connection accepted for session=%s", session_id)

    try:
        # Receive the question from the client
        data = await websocket.receive_text()
        try:
            payload = json.loads(data)
            question = payload.get("question", "").strip()
        except json.JSONDecodeError:
            question = data.strip()

        if not question:
            await websocket.send_text(
                json.dumps({"error": "Empty question received"})
            )
            await websocket.close()
            return

        logger.info("[WS] session=%s question=%.100s", session_id, question)

        # Cache check — send immediately on hit
        cache_key = _make_cache_key(session_id, question)
        cached = cache_get(cache_key)
        if cached:
            try:
                cached_result = json.loads(cached)
                logger.info("[WS] Cache HIT for session=%s", session_id)
                await websocket.send_text(
                    f"__RESULT__:{json.dumps({**cached_result, 'cache_hit': True, 'success': True})}"
                )
                log_usage.delay(session_id, question, 0, 0)
                return
            except Exception:
                pass  # Fall through to generation

        # RAG retrieval (runs in thread context via FastAPI's async execution)
        try:
            schema, rag_context = retrieve_relevant_schema(session_id, question)
        except (TimeoutError, RuntimeError) as e:
            await websocket.send_text(
                json.dumps({"error": str(e), "error_type": "schema_retrieval_error"})
            )
            return

        # Stream tokens from Claude
        result_data = None
        async for chunk in stream_sql_tokens(question=question, schema=schema):
            if chunk.startswith("__RESULT__:"):
                result_data = chunk[len("__RESULT__:"):]
                await websocket.send_text(chunk)
            else:
                await websocket.send_text(chunk)

        # Fire-and-forget Celery tasks after streaming completes
        if result_data:
            try:
                result = json.loads(result_data)
                if result.get("success") and result.get("sql"):
                    store_data = json.dumps({
                        "sql": result["sql"],
                        "rationale": result.get("rationale", ""),
                        "explanation": result.get("explanation", ""),
                        "tables_used": result.get("tables_used", []),
                    })
                    cache_result.delay(cache_key, store_data)
                log_usage.delay(
                    session_id,
                    question,
                    result.get("tokens_used", 0),
                    result.get("latency_ms", 0),
                )
            except Exception as e:
                logger.warning("[WS] Failed to dispatch Celery tasks: %s", e)

    except WebSocketDisconnect:
        logger.info("[WS] Client disconnected — session=%s", session_id)
    except Exception as e:
        logger.error("[WS] Unexpected error for session=%s: %s", session_id, e)
        try:
            await websocket.send_text(
                json.dumps({"error": str(e), "error_type": "internal_error"})
            )
        except Exception:
            pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
