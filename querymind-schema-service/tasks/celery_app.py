# querymind-schema-service/tasks/celery_app.py
"""
Celery application instance for the Schema Service.
Uses the shared celery_factory from querymind_shared and adds the beat schedule.
"""

from celery.schedules import crontab

from querymind_shared.celery_factory import create_celery_app
from core.config import settings

celery_app = create_celery_app(
    service_name="schema-service",
    amqp_url=settings.AMQP_URL,
)

# Periodic task: refresh caches that are about to expire
celery_app.conf.beat_schedule = {
    "refresh-expiring-schema-caches": {
        "task": "schema.tasks.refresh_expiring_caches",
        "schedule": settings.CACHE_REFRESH_INTERVAL,  # seconds (default: 3300 = 55 min)
        "options": {"queue": "schema-celery"},
    }
}

celery_app.conf.timezone = "UTC"

# Auto-discover tasks in this package
celery_app.autodiscover_tasks(["tasks"])
