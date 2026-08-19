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
# How long an event_id is remembered for Redis-side dedup. Only needs to
# outlive the window in which a crash-and-restart could replay a batch that
# was flushed but not yet committed - an hour is generous for that.
DEDUP_TTL_SECONDS = 3600


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
                    event_id=e.get("event_id"),
                    timestamp=parse_datetime(e["timestamp"]),
                    referrer=e.get("referrer", "")[:512],
                    ip_hash=e.get("ip_hash", ""),
                    user_agent=e.get("user_agent", "")[:512],
                )
            )

        if clicks:
            # ignore_conflicts: a redelivered event (at-least-once commit,
            # see class docstring) hits the event_id unique constraint and is
            # skipped instead of erroring the whole batch.
            Click.objects.bulk_create(clicks, ignore_conflicts=True)

        # Redis counters aren't naturally idempotent (a second INCR for the
        # same event double-counts), so gate them on a per-event_id dedup key
        # first: only events that "win" the SET NX get their counters applied.
        relevant = [e for e in events if e["code"] in code_to_id]

        dedup_pipe = redis_conn.pipeline()
        for e in relevant:
            dedup_pipe.set(f"dedup:click:{e.get('event_id')}", 1, nx=True, ex=DEDUP_TTL_SECONDS)
        dedup_results = dedup_pipe.execute() if relevant else []

        pipe = redis_conn.pipeline()
        for e, is_new in zip(relevant, dedup_results):
            if not is_new:
                continue
            code = e["code"]
            day = e["timestamp"][:10]
            referrer = e.get("referrer") or ""

            pipe.incr(f"stats:total:{code}")
            pipe.hincrby(f"stats:daily:{code}", day, 1)
            pipe.zincrby(f"stats:referrers:{code}", 1, referrer)
        pipe.execute()

        self.stdout.write(f"consume_clicks: flushed {len(clicks)} clicks")
