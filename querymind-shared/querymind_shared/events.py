# querymind-shared/querymind_shared/events.py
"""
All RabbitMQ event name constants for QueryMind.
This is the single source of truth for routing keys, exchange names, and Celery task names.
"""

# Exchange names
EXCHANGE = "querymind.events"
REPLY_EXCHANGE = ""  # default exchange for reply queues

# ── Schema Service events ──────────────────────────────────────────────────
SCHEMA_CONNECT_REQUEST      = "schema.connect.request"
SCHEMA_CONNECT_REPLY        = "schema.connect.reply"
SCHEMA_GET_REQUEST          = "schema.get.request"
SCHEMA_GET_REPLY            = "schema.get.reply"
SCHEMA_GET_TABLES_REQUEST   = "schema.get_tables.request"
SCHEMA_GET_TABLES_REPLY     = "schema.get_tables.reply"
SCHEMA_REFRESH_REQUEST      = "schema.refresh.request"
SCHEMA_REFRESH_REPLY        = "schema.refresh.reply"
SCHEMA_DISCONNECT_REQUEST   = "schema.disconnect.request"
SCHEMA_DISCONNECT_REPLY     = "schema.disconnect.reply"

# ── AI Service events ──────────────────────────────────────────────────────
AI_QUERY_GENERATE_REQUEST   = "ai.query.generate.request"
AI_QUERY_GENERATE_REPLY     = "ai.query.generate.reply"

# ── Execution Service events ───────────────────────────────────────────────
EXEC_INIT_REQUEST           = "exec.init.request"
EXEC_INIT_REPLY             = "exec.init.reply"
EXEC_RUN_REQUEST            = "exec.run.request"
EXEC_RUN_REPLY              = "exec.run.reply"
EXEC_HISTORY_REQUEST        = "exec.history.request"
EXEC_HISTORY_REPLY          = "exec.history.reply"

# ── Celery task names ──────────────────────────────────────────────────────
TASK_SCHEMA_WARM_CACHE          = "schema.tasks.warm_cache"
TASK_SCHEMA_REFRESH_EXPIRING    = "schema.tasks.refresh_expiring_caches"
TASK_AI_LOG_USAGE               = "ai.tasks.log_usage"
TASK_AI_CACHE_RESULT            = "ai.tasks.cache_result"
TASK_EXEC_PERSIST_HISTORY       = "exec.tasks.persist_history"
TASK_EXEC_ARCHIVE_RESULT        = "exec.tasks.archive_result"
