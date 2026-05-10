import logging
import threading
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from core.config import settings

logger = logging.getLogger(__name__)

# Thread-safe in-memory session engine store
# { session_id: Engine }
_store: dict[str, Engine] = {}
_lock = threading.Lock()


def create_session_engine(session_id: str, connection_string: str) -> Engine:
    """
    Create a SQLAlchemy engine for the given connection string and store it
    under session_id. Validates the connection with a test query before storing.

    Args:
        session_id: User's session identifier.
        connection_string: PostgreSQL connection string from the user.

    Returns:
        The created Engine.

    Raises:
        Exception: If the connection cannot be established.
    """
    engine = create_engine(
        connection_string,
        pool_size=settings.DB_POOL_SIZE,
        max_overflow=settings.DB_MAX_OVERFLOW,
        pool_timeout=settings.DB_POOL_TIMEOUT,
        pool_pre_ping=True,          # detect stale connections before use
        echo=False,
    )

    # Validate the connection before storing
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    with _lock:
        # Dispose old engine if a session is being re-initialised
        if session_id in _store:
            try:
                _store[session_id].dispose()
            except Exception:
                pass
        _store[session_id] = engine

    logger.info("Session engine created and stored for session_id=%s", session_id)
    return engine


def get_session_engine(session_id: str) -> Optional[Engine]:
    """Return the engine for a session, or None if not found."""
    with _lock:
        return _store.get(session_id)


def remove_session_engine(session_id: str) -> None:
    """Dispose and remove the engine for a session."""
    with _lock:
        engine = _store.pop(session_id, None)
    if engine:
        try:
            engine.dispose()
            logger.info("Session engine disposed for session_id=%s", session_id)
        except Exception as e:
            logger.warning("Error disposing engine for session_id=%s: %s", session_id, e)


def active_sessions() -> list[str]:
    """Return a list of currently active session IDs."""
    with _lock:
        return list(_store.keys())
