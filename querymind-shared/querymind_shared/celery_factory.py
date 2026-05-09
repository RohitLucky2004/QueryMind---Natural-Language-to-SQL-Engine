# querymind-shared/querymind_shared/celery_factory.py
"""
Shared Celery application factory.
Each Python microservice calls create_celery_app() to get a configured Celery instance.
"""

from celery import Celery


def create_celery_app(service_name: str, amqp_url: str) -> Celery:
    """
    Create and configure a Celery application for a QueryMind service.

    Args:
        service_name: Name of the service (e.g. "schema-service").
        amqp_url:     RabbitMQ AMQP connection URL.

    Returns:
        Configured Celery application instance.
    """
    app = Celery(service_name)
    app.conf.update(
        broker_url=amqp_url,
        result_backend=None,          # fire-and-forget — no result store needed
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        task_acks_late=True,          # ack after task completes, not on receipt
        worker_prefetch_multiplier=1, # fair dispatch — one task per worker at a time
        task_routes={
            "schema.tasks.*":  {"queue": "schema-celery"},
            "ai.tasks.*":      {"queue": "ai-celery"},
            "exec.tasks.*":    {"queue": "exec-celery"},
        },
        task_default_queue="default",
        broker_connection_retry_on_startup=True,
    )
    return app
