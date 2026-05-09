# querymind-shared/querymind_shared/consumer.py
"""
RabbitMQ consumer base class.
Used by all Python microservices to receive messages from the event bus.
"""

import json
import logging
from typing import Callable

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from querymind_shared.events import EXCHANGE

logger = logging.getLogger(__name__)


class Consumer:
    """
    Blocking RabbitMQ consumer.

    Declares a durable queue, binds it to the topic exchange with the provided
    routing keys, and enters a blocking consume loop that dispatches to a handler.

    Usage:
        consumer = Consumer(amqp_url, "my-service", ["event.one", "event.two"])
        consumer.start_consuming(my_handler)
        # my_handler(routing_key: str, body: dict, properties: BasicProperties) -> None
    """

    def __init__(self, amqp_url: str, queue_name: str, routing_keys: list[str]) -> None:
        self._amqp_url = amqp_url
        self._queue_name = queue_name
        self._routing_keys = routing_keys
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None
        self._consuming = False
        self._connect()

    def _connect(self) -> None:
        params = pika.URLParameters(self._amqp_url)
        params.heartbeat = 60
        params.blocked_connection_timeout = 300
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()

        # Declare exchange
        self._channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type="topic",
            durable=True,
        )

        # Declare durable queue
        self._channel.queue_declare(
            queue=self._queue_name,
            durable=True,
            arguments={"x-max-priority": 0},
        )

        # Bind each routing key
        for rk in self._routing_keys:
            self._channel.queue_bind(
                queue=self._queue_name,
                exchange=EXCHANGE,
                routing_key=rk,
            )
            logger.debug("Bound queue '%s' → routing_key='%s'", self._queue_name, rk)

        # Fair dispatch: one message at a time per consumer
        self._channel.basic_qos(prefetch_count=1)
        logger.info(
            "Consumer ready — queue='%s', routing_keys=%s",
            self._queue_name,
            self._routing_keys,
        )

    def start_consuming(self, handler: Callable[[str, dict, BasicProperties], None]) -> None:
        """
        Enter the blocking consume loop.
        Calls handler(routing_key, body_dict, properties) for each message.
        Acknowledges the message after a successful handler call.
        Nacks (without requeue) on handler exception to avoid poison-message loops.
        """
        self._consuming = True

        def _on_message(
            channel: BlockingChannel,
            method: Basic.Deliver,
            properties: BasicProperties,
            body: bytes,
        ) -> None:
            routing_key = method.routing_key
            try:
                body_dict = json.loads(body)
            except json.JSONDecodeError as exc:
                logger.error("Failed to decode message body on [%s]: %s", routing_key, exc)
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            try:
                handler(routing_key, body_dict, properties)
                channel.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as exc:
                logger.exception("Handler raised on [%s]: %s", routing_key, exc)
                # Nack without requeue to avoid poison loops
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        self._channel.basic_consume(
            queue=self._queue_name,
            on_message_callback=_on_message,
        )

        logger.info("Starting consume loop on queue='%s'", self._queue_name)
        try:
            self._channel.start_consuming()
        except Exception as exc:
            logger.error("Consume loop exited: %s", exc)
        finally:
            self._consuming = False

    def close(self) -> None:
        """Stop consuming and close the connection."""
        try:
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
        except Exception:
            pass
        try:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
        except Exception:
            pass
        logger.info("Consumer closed for queue='%s'", self._queue_name)

    @property
    def is_consuming(self) -> bool:
        return self._consuming
