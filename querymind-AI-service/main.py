import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from core.config import settings
from sync.events import QUEUE_NAME, ROUTING_KEYS
from querymind_shared.consumer import Consumer
from querymind_shared.publisher import Publisher
from querymind_shared.schemas import AIQueryGenerateRequest
from querymind_shared.events import AI_QUERY_GENERATE_REQUEST
from handlers.query_handler import handle_generate
from routers import query_ws, query_rest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Module-level publisher shared by the consumer thread
_publisher: Publisher | None = None
_consumer_thread: threading.Thread | None = None


def _dispatch(routing_key: str, body: dict, properties) -> None:
    """
    Route an incoming RabbitMQ message to the correct handler.
    All handlers publish their reply via the shared publisher.
    """
    reply_to = getattr(properties, "reply_to", None)
    if not reply_to:
        logger.warning("Message with no reply_to — dropping. routing_key=%s", routing_key)
        return

    if routing_key == AI_QUERY_GENERATE_REQUEST:
        try:
            payload = AIQueryGenerateRequest(**body)
        except Exception as e:
            logger.error("Failed to parse AIQueryGenerateRequest: %s", e)
            return
        handle_generate(payload=payload, reply_to=reply_to, publisher=_publisher)
    else:
        logger.warning("Unknown routing key: %s", routing_key)


def _start_consumer() -> None:
    """Blocking consumer loop — runs in a daemon thread."""
    global _publisher
    logger.info("Starting RabbitMQ consumer on queue '%s'", QUEUE_NAME)
    try:
        _publisher = Publisher(amqp_url=settings.AMQP_URL)
        consumer = Consumer(
            amqp_url=settings.AMQP_URL,
            queue_name=QUEUE_NAME,
            routing_keys=ROUTING_KEYS,
        )
        consumer.start_consuming(handler=_dispatch)
    except Exception as e:
        logger.error("Consumer thread crashed: %s", e, exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan: start the RabbitMQ consumer daemon thread on startup."""
    global _consumer_thread

    _consumer_thread = threading.Thread(
        target=_start_consumer,
        daemon=True,
        name="rabbitmq-consumer",
    )
    _consumer_thread.start()
    logger.info("RabbitMQ consumer thread started")

    # Expose thread reference to health endpoint
    query_rest.set_consumer_thread(_consumer_thread)

    yield

    # Shutdown — daemon thread exits automatically with the process
    logger.info("AI Query Service shutting down")


app = FastAPI(
    title="QueryMind AI Query Service",
    description="Generates validated SQL from natural language using Claude + RAG",
    version="2.0.0",
    lifespan=lifespan,
)

# Routers
app.include_router(query_ws.router)
app.include_router(query_rest.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=settings.PORT, reload=False)
