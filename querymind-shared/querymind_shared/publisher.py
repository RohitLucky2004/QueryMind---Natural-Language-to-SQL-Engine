# querymind-shared/querymind_shared/publisher.py
"""
RabbitMQ publisher with correlation_id support.
Used by all services to publish events and replies.
"""

import json
import logging

import pika
from pika.spec import BasicProperties
from pydantic import BaseModel

from querymind_shared.events import EXCHANGE

logger = logging.getLogger(__name__)


class Publisher:
    """Thread-safe RabbitMQ publisher.

    Declares the topic exchange on first connect.
    All publish operations use basic_publish to the exchange with a routing key.
    Reply publishing uses the direct exchange (empty string) targeting the reply_to queue.
    """

    def __init__(self, amqp_url: str) -> None:
        self._amqp_url = amqp_url
        self._connection: pika.BlockingConnection | None = None
        self._channel: pika.adapters.blocking_connection.BlockingChannel | None = None
        self._connect()

    def _connect(self) -> None:
        params = pika.URLParameters(self._amqp_url)
        params.heartbeat = 60
        params.blocked_connection_timeout = 300
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type="topic",
            durable=True,
        )
        logger.info("Publisher connected to RabbitMQ, exchange='%s'", EXCHANGE)

    def _ensure_connected(self) -> None:
        if self._connection is None or self._connection.is_closed:
            logger.warning("Publisher connection lost — reconnecting")
            self._connect()

    def publish(
        self,
        routing_key: str,
        payload: BaseModel,
        reply_to: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Publish a Pydantic model to the querymind.events exchange."""
        self._ensure_connected()
        body = payload.model_dump_json().encode()
        properties = BasicProperties(
            content_type="application/json",
            delivery_mode=2,  # persistent
            reply_to=reply_to,
            correlation_id=correlation_id or getattr(payload, "correlation_id", None),
        )
        self._channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing_key,
            body=body,
            properties=properties,
        )
        logger.debug("Published [%s] correlation_id=%s", routing_key, properties.correlation_id)

    def publish_to_reply_queue(
        self,
        reply_to: str,
        payload: BaseModel,
        correlation_id: str | None = None,
    ) -> None:
        """Publish a reply directly to a reply_to queue (uses default exchange)."""
        self._ensure_connected()
        body = payload.model_dump_json().encode()
        properties = BasicProperties(
            content_type="application/json",
            delivery_mode=1,  # transient — replies don't need persistence
            correlation_id=correlation_id,
        )
        self._channel.basic_publish(
            exchange="",       # default exchange — routes by queue name
            routing_key=reply_to,
            body=body,
            properties=properties,
        )
        logger.debug("Reply published to queue='%s' correlation_id=%s", reply_to, correlation_id)

    def publish_raw(
        self,
        reply_to: str,
        body_json: str,
        correlation_id: str | None = None,
    ) -> None:
        """Publish a raw JSON string to a reply queue (used for error replies)."""
        self._ensure_connected()
        properties = BasicProperties(
            content_type="application/json",
            delivery_mode=1,
            correlation_id=correlation_id,
        )
        self._channel.basic_publish(
            exchange="",
            routing_key=reply_to,
            body=body_json.encode(),
            properties=properties,
        )

    def close(self) -> None:
        try:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
        except Exception as exc:
            logger.warning("Publisher close error: %s", exc)
