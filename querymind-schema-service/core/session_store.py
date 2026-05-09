# querymind-schema-service/core/session_store.py
"""
In-memory store for SQLAlchemy engine instances keyed by session_id.
Thread-safe via a RLock.
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import Engine

logger = logging.getLogger(__name__)


@dataclass
class SessionEntry:
    session_id: str
    engine: Engine
    database_name: str
    connection_string: str
    connected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class SessionStore:
    """Thread-safe in-memory store for database engine sessions."""

    def __init__(self) -> None:
        self._sessions: dict[str, SessionEntry] = {}
        self._lock = threading.RLock()

    def add(self, session_id: str, engine: Engine, database_name: str, connection_string: str) -> SessionEntry:
        entry = SessionEntry(
            session_id=session_id,
            engine=engine,
            database_name=database_name,
            connection_string=connection_string,
        )
        with self._lock:
            self._sessions[session_id] = entry
        logger.info("Session stored: %s → %s", session_id, database_name)
        return entry

    def get(self, session_id: str) -> SessionEntry | None:
        with self._lock:
            return self._sessions.get(session_id)

    def remove(self, session_id: str) -> bool:
        with self._lock:
            entry = self._sessions.pop(session_id, None)
            if entry:
                try:
                    entry.engine.dispose()
                except Exception as exc:
                    logger.warning("Error disposing engine for %s: %s", session_id, exc)
                logger.info("Session removed: %s", session_id)
                return True
            return False

    def all_session_ids(self) -> list[str]:
        with self._lock:
            return list(self._sessions.keys())

    def has(self, session_id: str) -> bool:
        with self._lock:
            return session_id in self._sessions

    def count(self) -> int:
        with self._lock:
            return len(self._sessions)


# Module-level singleton used across the service
session_store = SessionStore()
