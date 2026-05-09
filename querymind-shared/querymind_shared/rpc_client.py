# querymind-shared/querymind_shared/rpc_client.py
"""
Synchronous RPC client using RabbitMQ direct reply-to.

Used by services that need to make a blocking request-reply call to another
service via the message bus — e.g. AI Service calling Schema Service.

Uses amq.rabbitmq.reply-to (RabbitMQ's built-in ephemeral reply queue) so no
temporary queue creation is needed.
"""

import json
import logging
import threading
import uuid
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from pydantic import BaseModel

from querymind_shared.events import EXCHANGE

logger = logging.getLogger(__name__)

_DIRECT_REPLY_QUEUE = "amq.rabbitmq.reply-to"


class TimeoutError(Exception):
    """Raised when an RPC call does not receive a reply within the timeout."""


class RPCClient:
    """
    Synchronous RPC over RabbitMQ using direct reply-to.

    Each call publishes a message with reply_to="amq.rabbitmq.reply-to" and
    a unique correlation_id, then blocks until a matching reply arrives or
    the timeout elapses.

    Thread-safety: Use one RPCClient per thread (pika BlockingConnection is not thread-safe).
    """

    def __init__(self, amqp_url: str) -> None:
        self._amqp_url = amqp_url
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None
        self._reply: dict | None = None
        self._correlation_id: str | None = None
        self._event = threading.Event()
        self._connect()

    def _connect(self) -> None:
        params = pika.URLParameters(self._amqp_url)
        params.heartbeat = 60
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()

        # Declare exchange so it exists even if services haven't started yet
        self._channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type="topic",
            durable=True,
        )

        # Start consuming from the direct reply-to queue
        self._channel.basic_consume(
            queue=_DIRECT_REPLY_QUEUE,
            on_message_callback=self._on_reply,
            auto_ack=True,
        )
        logger.debug("RPCClient connected and listening on %s", _DIRECT_REPLY_QUEUE)

    def _on_reply(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        if properties.correlation_id == self._correlation_id:
            try:
                self._reply = json.loads(body)
            except json.JSONDecodeError as exc:
                logger.error("RPCClient: failed to decode reply: %s", exc)
                self._reply = {}
            self._event.set()

    def call(
        self,
        routing_key: str,
        payload: BaseModel,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """
        Publish a request and block until the reply arrives.

        Args:
            routing_key: The event routing key to publish on (e.g. SCHEMA_GET_REQUEST).
            payload:     A Pydantic BaseModel to serialize as the message body.
            timeout:     Seconds to wait for a reply before raising TimeoutError.

        Returns:
            The parsed JSON reply body as a dict.

        Raises:
            TimeoutError: If no matching reply arrives within `timeout` seconds.
        """
        correlation_id = str(uuid.uuid4())
        self._correlation_id = correlation_id
        self._reply = None
        self._event.clear()

        body = payload.model_dump_json().encode()
        properties = BasicProperties(
            content_type="application/json",
            correlation_id=correlation_id,
            reply_to=_DIRECT_REPLY_QUEUE,
            delivery_mode=1,  # transient
        )

        self._channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing_key,
            body=body,
            properties=properties,
        )
        logger.debug("RPCClient: sent [%s] correlation_id=%s", routing_key, correlation_id)

        # Pump the event loop until our reply lands or timeout
        deadline = timeout
        interval = 0.05  # 50ms poll
        while deadline > 0:
            self._connection.process_data_events(time_limit=interval)
            if self._event.is_set():
                logger.debug(
                    "RPCClient: reply received for [%s] correlation_id=%s",
                    routing_key,
                    correlation_id,
                )
                return self._reply  # type: ignore[return-value]
            deadline -= interval

        raise TimeoutError(
            f"RPC call to [{routing_key}] timed out after {timeout}s "
            f"(correlation_id={correlation_id})"
        )

    def close(self) -> None:
        try:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
        except Exception as exc:
            logger.warning("RPCClient close error: %s", exc)
