import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from core.config import settings
from sync.events import QUEUE_NAME, ROUTING_KEYS
from querymind_shared.consumer import Consumer
from querymind_shared.publisher import Publisher
from querymind_shared.schemas import (
    ExecInitRequest,
    ExecRunRequest,
    ExecHistoryRequest,
)
from querymind_shared.events import (
    EXEC_INIT_REQUEST,
    EXEC_RUN_REQUEST,
    EXEC_HISTORY_REQUEST,
)
from handlers.execution_handler import handle_init, handle_run, handle_history
from routers import health as health_router

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
    Each handler publishes its reply via the shared publisher.
    """
    reply_to = getattr(properties, "reply_to", None)
    if not reply_to:
        logger.warning(
            "Message with no reply_to — dropping. routing_key=%s", routing_key
        )
        return

    try:
        if routing_key == EXEC_INIT_REQUEST:
            payload = ExecInitRequest(**body)
            handle_init(payload=payload, reply_to=reply_to, publisher=_publisher)

        elif routing_key == EXEC_RUN_REQUEST:
            payload = ExecRunRequest(**body)
            handle_run(payload=payload, reply_to=reply_to, publisher=_publisher)

        elif routing_key == EXEC_HISTORY_REQUEST:
            payload = ExecHistoryRequest(**body)
            handle_history(payload=payload, reply_to=reply_to, publisher=_publisher)

        else:
            logger.warning("Unknown routing key received: %s", routing_key)

    except Exception as e:
        logger.error(
            "Dispatch error for routing_key=%s: %s", routing_key, e, exc_info=True
        )
        # nack is handled by the Consumer base class — this exception propagates
        # to trigger basic_nack(requeue=False) to prevent poison message loops.
        raise


def _start_consumer() -> None:
    """Blocking consumer loop — runs in a daemon thread alongside FastAPI."""
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
    """
    FastAPI lifespan context manager.
    Starts the RabbitMQ consumer in a daemon thread on startup.
    The daemon thread exits automatically when the process exits.
    """
    global _consumer_thread

    _consumer_thread = threading.Thread(
        target=_start_consumer,
        daemon=True,
        name="rabbitmq-consumer",
    )
    _consumer_thread.start()
    logger.info("RabbitMQ consumer thread started (daemon=True)")

    # Expose thread reference to the health endpoint
    health_router.set_consumer_thread(_consumer_thread)

    yield

    logger.info("Query Execution Service shutting down")


app = FastAPI(
    title="QueryMind Query Execution Service",
    description=(
        "Executes validated SQL against user-connected PostgreSQL databases "
        "with READ ONLY transaction safety and statement timeout enforcement."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

# Routers
app.include_router(health_router.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=settings.PORT, reload=False)
