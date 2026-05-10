from querymind_shared.celery_factory import create_celery_app
from core.config import settings

celery_app = create_celery_app(
    service_name="exec-service",
    amqp_url=settings.AMQP_URL,
)
