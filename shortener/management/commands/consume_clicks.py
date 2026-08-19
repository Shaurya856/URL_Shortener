import json
import logging
import time

from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils.dateparse import parse_datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from shortener.models import Click, ShortURL
from shortener.redis_client import get_redis

logger = logging.getLogger(__name__)

BATCH_SIZE = 100
BATCH_TIMEOUT_SECONDS = 5


class Command(BaseCommand):
    """Long-running consumer for the click_events Kafka topic.

    Runs as a separate process/container (same codebase, different
    entrypoint: `python manage.py consume_clicks`). Batches inserts into
    Postgres and updates rolling Redis aggregates so the stats pages never
    have to touch Kafka or scan raw click rows at request time.

    Offsets are committed manually, only after a batch's _flush() succeeds
    (enable_auto_commit=False) — so a crash mid-batch replays those events
    on restart instead of skipping them, at the cost of possible duplicate
    processing (at-least-once, not exactly-once).
    """

    help = "Consume click_events from Kafka, batch-write to Postgres, update Redis aggregates"

    def handle(self, *args, **options):
        consumer = self._connect_with_retry()
        redis_conn = get_redis()

        buffer = []
        last_flush = time.time()

        self.stdout.write(self.style.SUCCESS("consume_clicks: listening on click_events"))

        while True:
            for message in consumer:
                buffer.append(message.value)

                should_flush = (
                    len(buffer) >= BATCH_SIZE
                    or (time.time() - last_flush) >= BATCH_TIMEOUT_SECONDS
                )
                if should_flush:
                    self._flush(buffer, redis_conn)
                    consumer.commit()
                    buffer = []
                    last_flush = time.time()

            # consumer_timeout_ms fired with no messages: flush whatever we have.
            if buffer:
                self._flush(buffer, redis_conn)
                consumer.commit()
                buffer = []
                last_flush = time.time()

    def _connect_with_retry(self):
        while True:
            try:
                return KafkaConsumer(
                    settings.KAFKA_CLICK_TOPIC,
                    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                    group_id="analytics-consumer",
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    consumer_timeout_ms=BATCH_TIMEOUT_SECONDS * 1000,
                )
            except KafkaError as exc:
                logger.warning("Kafka unavailable, retrying in 3s: %s", exc)
                time.sleep(3)

    def _flush(self, events, redis_conn):
        if not events:
            return

        codes = {e["code"] for e in events}
        code_to_id = dict(ShortURL.objects.filter(code__in=codes).values_list("code", "id"))

        clicks = []
        for e in events:
            short_id = code_to_id.get(e["code"])
            if short_id is None:
                continue
            clicks.append(
                Click(
                    short_url_id=short_id,
                    timestamp=parse_datetime(e["timestamp"]),
                    referrer=e.get("referrer", "")[:512],
                    ip_hash=e.get("ip_hash", ""),
                    user_agent=e.get("user_agent", "")[:512],
                )
            )

        if clicks:
            Click.objects.bulk_create(clicks)

        pipe = redis_conn.pipeline()
        for e in events:
            if e["code"] not in code_to_id:
                continue
            code = e["code"]
            day = e["timestamp"][:10]
            referrer = e.get("referrer") or ""

            pipe.incr(f"stats:total:{code}")
            pipe.hincrby(f"stats:daily:{code}", day, 1)
            pipe.zincrby(f"stats:referrers:{code}", 1, referrer)
        pipe.execute()

        self.stdout.write(f"consume_clicks: flushed {len(clicks)} clicks")
