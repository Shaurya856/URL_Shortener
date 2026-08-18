import json
import logging

from django.conf import settings
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

_producer = None
_producer_failed = False


def get_producer():
    """Lazily create a singleton KafkaProducer.

    If Kafka is unreachable we mark it failed once and stop retrying on
    every request — the redirect path must never be slowed down or broken
    by the analytics pipeline being unavailable.
    """
    global _producer, _producer_failed
    if _producer_failed:
        return None
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # acks='all': the write is only considered successful once
                # every in-sync replica (min.insync.replicas=2 on the topic)
                # has it, so a published click survives a single broker
                # dying. This does NOT make the redirect path slower —
                # send() is still fire-and-forget, we never block on the
                # returned future. It does mean a batch takes a little
                # longer to be acknowledged and freed from the producer's
                # internal buffer, so max_block_ms below is what actually
                # bounds worst-case impact on the request thread if the
                # buffer fills up during a broker outage.
                acks="all",
                retries=3,
                linger_ms=20,
                request_timeout_ms=2000,
                max_block_ms=500,
            )
        except KafkaError as exc:
            logger.warning("Kafka producer unavailable: %s", exc)
            _producer_failed = True
            return None
    return _producer


def publish_click_event(event: dict) -> None:
    """Fire-and-forget publish. Never raises, never blocks the redirect."""
    producer = get_producer()
    if producer is None:
        return
    try:
        producer.send(settings.KAFKA_CLICK_TOPIC, value=event)
    except KafkaError as exc:
        logger.warning("Failed to publish click event: %s", exc)
