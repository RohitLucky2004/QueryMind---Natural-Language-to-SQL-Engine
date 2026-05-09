# querymind-shared/querymind_shared/__init__.py
"""
querymind_shared — Shared event bus infrastructure for QueryMind microservices.

Provides:
  - events.py        — All RabbitMQ event name constants
  - schemas.py       — Pydantic message models for all events
  - publisher.py     — RabbitMQ publisher
  - consumer.py      — RabbitMQ consumer base class
  - rpc_client.py    — Synchronous RPC over RabbitMQ (direct reply-to)
  - celery_factory.py — Shared Celery app factory
"""

__version__ = "2.0.0"
