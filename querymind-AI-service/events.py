# querymind-ai-service/sync/events.py
"""
AI Query Service — RabbitMQ Event Contract
==========================================
This file declares every event this service CONSUMES and every reply it PUBLISHES.
This service does NOT call Schema Service via HTTP. It uses RabbitMQ RPC instead.

CONSUMES (queue: ai-service):
  ai.query.generate.request   → AIQueryGenerateRequest

PUBLISHES (to reply_to queue):
  AIQueryGenerateReply

INTERNAL RPC CALLS (made by this service as a client):
  → schema.get_tables.request  (to get table list for RAG relevance detection)
  → schema.get.request         (to get targeted schema chunks for prompt injection)

CELERY TASKS (queue: ai-celery):
  ai.tasks.log_usage     → Fire-and-forget: logs model usage (tokens, latency) to Redis
  ai.tasks.cache_result  → Fire-and-forget: stores generated SQL in Redis cache after generation
"""

from querymind_shared.events import AI_QUERY_GENERATE_REQUEST

# Queue this service listens on
QUEUE_NAME = "ai-service"

# All routing keys this service is bound to
ROUTING_KEYS = [AI_QUERY_GENERATE_REQUEST]
