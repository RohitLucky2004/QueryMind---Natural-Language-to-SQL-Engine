# querymind-execution-service/sync/events.py
"""
Execution Service — RabbitMQ Event Contract
===========================================
This service does NOT call Schema Service via HTTP for connection strings.
Connection strings are passed in via exec.init.request at session creation time.

CONSUMES (queue: exec-service):
  exec.init.request      → ExecInitRequest     (registers session + connection_string)
  exec.run.request       → ExecRunRequest      (runs a validated SQL query)
  exec.history.request   → ExecHistoryRequest  (returns last 20 executed queries)

PUBLISHES (to reply_to queue):
  ExecInitReply
  ExecRunReply
  ExecHistoryReply

CELERY TASKS (queue: exec-celery):
  exec.tasks.persist_history  → Appends execution record to Redis history list
  exec.tasks.archive_result   → Stores full result set in Redis with short TTL (for re-pagination)
"""

from querymind_shared.events import (
    EXEC_INIT_REQUEST,
    EXEC_RUN_REQUEST,
    EXEC_HISTORY_REQUEST,
)

# Queue this service listens on
QUEUE_NAME = "exec-service"

# All routing keys this service is bound to
ROUTING_KEYS = [
    EXEC_INIT_REQUEST,
    EXEC_RUN_REQUEST,
    EXEC_HISTORY_REQUEST,
]
