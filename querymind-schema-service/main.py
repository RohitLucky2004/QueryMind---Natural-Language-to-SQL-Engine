# querymind-schema-service/main.py
"""
Schema Introspection Service — FastAPI entrypoint.

- Starts a background thread running the RabbitMQ consumer on startup.
- Exposes GET /health for Docker health checks and Gateway monitoring.
- All schema operations are handled via RabbitMQ messages (no inter-service HTTP).
"""

import json
import logging
import threading
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from querymind_shared.consumer import Consumer
from querymind_shared.publisher import Publisher
from querymind_shared.schemas import (
    SchemaConnectRequest,
    SchemaGetRequest,
    SchemaGetTablesRequest,
    SchemaRefreshRequest,
    SchemaDisconnectRequest,
)
from querymind_shared.events import (
    SCHEMA_CONNECT_REQUEST,
    SCHEMA_GET_REQUEST,
    SCHEMA_GET_TABLES_REQUEST,
    SCHEMA_REFRESH_REQUEST,
    SCHEMA_DISCONNECT_REQUEST,
)

from core.config import settings
from core.redis_client import RedisClient
from handlers.schema_handler import (
    handle_connect,
    handle_get,
    handle_get_tables,
    handle_refresh,
    handle_disconnect,
)
from sync.events import QUEUE_NAME, ROUTING_KEYS

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# Module-level references so we can close them on shutdown
_consumer: Consumer | None = None
_publisher: Publisher | None = None
_consumer_thread: threading.Thread | None = None


# ---------------------------------------------------------------------------
# RabbitMQ message dispatcher
# ---------------------------------------------------------------------------

def _dispatch(routing_key: str, body: dict, properties) -> None:
    """
    Called by the Consumer for each message.
    Routes to the correct handler and publishes the reply.
    """
    global _publisher

    reply_to = getattr(properties, "reply_to", None)
    correlation_id = getattr(properties, "correlation_id", None)

    logger.info("Received [%s] correlation_id=%s reply_to=%s", routing_key, correlation_id, reply_to)

    try:
        if routing_key == SCHEMA_CONNECT_REQUEST:
            reply = handle_connect(SchemaConnectRequest(**body))

        elif routing_key == SCHEMA_GET_REQUEST:
            reply = handle_get(SchemaGetRequest(**body))

        elif routing_key == SCHEMA_GET_TABLES_REQUEST:
            reply = handle_get_tables(SchemaGetTablesRequest(**body))

        elif routing_key == SCHEMA_REFRESH_REQUEST:
            reply = handle_refresh(SchemaRefreshRequest(**body))

        elif routing_key == SCHEMA_DISCONNECT_REQUEST:
            reply = handle_disconnect(SchemaDisconnectRequest(**body))

        else:
            logger.warning("Unknown routing key: %s — ignoring", routing_key)
            return

    except Exception as exc:
        logger.exception("Handler raised an exception for routing_key=%s", routing_key)
        # Publish a generic error reply if we can
        if reply_to and _publisher:
            error_payload = {
                "success": False,
                "error": f"Internal handler error: {exc}",
                "correlation_id": body.get("correlation_id", ""),
                "session_id": body.get("session_id", ""),
                "timestamp": "",
            }
            try:
                _publisher.publish_raw(reply_to, json.dumps(error_payload), correlation_id=correlation_id)
            except Exception:
                pass
        return

    # Publish reply
    if reply_to and _publisher:
        try:
            _publisher.publish_to_reply_queue(reply_to, reply, correlation_id=correlation_id)
            logger.info(
                "Reply published for [%s] correlation_id=%s success=%s",
                routing_key,
                correlation_id,
                getattr(reply, "success", "?"),
            )
        except Exception as exc:
            logger.error("Failed to publish reply for [%s]: %s", routing_key, exc)
    else:
        logger.warning("No reply_to for routing_key=%s — reply dropped", routing_key)


# ---------------------------------------------------------------------------
# Consumer thread
# ---------------------------------------------------------------------------

def _start_consumer() -> None:
    """Runs in a background daemon thread; starts the blocking consume loop."""
    global _consumer, _publisher

    logger.info("Connecting to RabbitMQ: %s", settings.AMQP_URL)
    _consumer = Consumer(
        amqp_url=settings.AMQP_URL,
        queue_name=QUEUE_NAME,
        routing_keys=ROUTING_KEYS,
    )
    _publisher = Publisher(amqp_url=settings.AMQP_URL)

    logger.info("Schema Service RabbitMQ consumer starting on queue '%s'", QUEUE_NAME)
    _consumer.start_consuming(_dispatch)


# ---------------------------------------------------------------------------
# FastAPI lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _consumer_thread

    # Startup: launch consumer in a daemon thread
    _consumer_thread = threading.Thread(target=_start_consumer, daemon=True, name="rabbitmq-consumer")
    _consumer_thread.start()
    logger.info("RabbitMQ consumer thread started.")

    yield

    # Shutdown: stop consumer gracefully
    logger.info("Shutting down Schema Service...")
    if _consumer:
        try:
            _consumer.close()
        except Exception:
            pass
    if _publisher:
        try:
            _publisher.close()
        except Exception:
            pass
    RedisClient.close()
    logger.info("Schema Service shutdown complete.")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="QueryMind — Schema Introspection Service",
    version="2.0.0",
    description="Listens on RabbitMQ queue 'schema-service' for schema operations. "
                "Exposes only /health via HTTP.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

@app.get("/health", tags=["health"])
def health_check():
    """
    Health check endpoint.
    Checked by Docker, Kubernetes liveness probes, and the API Gateway.
    """
    rabbitmq_status = "connected" if (
        _consumer is not None and _consumer_thread is not None and _consumer_thread.is_alive()
    ) else "disconnected"

    redis_ok = RedisClient.ping()

    from core.session_store import session_store
    return {
        "service": "schema-introspection-service",
        "status": "ok",
        "port": settings.PORT,
        "rabbitmq": rabbitmq_status,
        "redis": "ok" if redis_ok else "error",
        "active_sessions": session_store.count(),
    }


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.PORT,
        log_level=settings.LOG_LEVEL.lower(),
        reload=False,
    )
