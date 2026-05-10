import logging

from fastapi import APIRouter

from core.session_store import active_sessions

router = APIRouter()
logger = logging.getLogger(__name__)

_consumer_thread_ref = None


def set_consumer_thread(thread):
    global _consumer_thread_ref
    _consumer_thread_ref = thread


@router.get("/health")
async def health():
    """
    Health check endpoint.

    Reports:
    - RabbitMQ consumer thread liveness
    - Number of active database sessions
    - Celery status (placeholder — extend with result backend probe if needed)
    """
    rabbitmq_status = "connected"

    if _consumer_thread_ref is not None and not _consumer_thread_ref.is_alive():
        rabbitmq_status = "disconnected"
        logger.warning("Health check: consumer thread is NOT alive")

    sessions = active_sessions()

    return {
        "service": "query-execution-service",
        "status": "ok",
        "port": 8003,
        "rabbitmq": rabbitmq_status,
        "celery": "ok",
        "active_sessions": len(sessions),
    }
