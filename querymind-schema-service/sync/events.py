# querymind-schema-service/sync/events.py
"""
Schema Service — RabbitMQ Event Contract
========================================
This file declares every event this service CONSUMES and every reply it PUBLISHES.
No other service may call this service via HTTP. All communication goes through these events.

CONSUMES (queue: schema-service):
  schema.connect.request      → SchemaConnectRequest
  schema.get.request          → SchemaGetRequest
  schema.get_tables.request   → SchemaGetTablesRequest
  schema.refresh.request      → SchemaRefreshRequest
  schema.disconnect.request   → SchemaDisconnectRequest

PUBLISHES (to reply_to queue in message properties):
  SchemaConnectReply
  SchemaGetReply
  SchemaGetTablesReply
  SchemaRefreshReply
  SchemaDisconnectReply

CELERY TASKS (queue: schema-celery):
  schema.tasks.warm_cache              → Triggered after connect; pre-builds all Redis chunks in background
  schema.tasks.refresh_expiring_caches → Periodic (every 55 min); re-warms caches about to expire
"""

from querymind_shared.events import (
    SCHEMA_CONNECT_REQUEST,
    SCHEMA_GET_REQUEST,
    SCHEMA_GET_TABLES_REQUEST,
    SCHEMA_REFRESH_REQUEST,
    SCHEMA_DISCONNECT_REQUEST,
)

# Queue this service listens on
QUEUE_NAME = "schema-service"

# All routing keys this service is bound to
ROUTING_KEYS = [
    SCHEMA_CONNECT_REQUEST,
    SCHEMA_GET_REQUEST,
    SCHEMA_GET_TABLES_REQUEST,
    SCHEMA_REFRESH_REQUEST,
    SCHEMA_DISCONNECT_REQUEST,
]
