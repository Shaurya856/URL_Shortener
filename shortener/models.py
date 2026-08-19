from django.conf import settings
from django.db import models


class IdCounter(models.Model):
    """Single-row central counter backing pre-allocated ID range claiming.

    Each app process claims a block of `next_value` via one atomic
    `UPDATE ... RETURNING` (see shortener.id_allocator) instead of hitting
    this table once per shorten request. See id_allocator.py for how the
    claimed range is then handed out in-process.
    """

    id = models.PositiveSmallIntegerField(primary_key=True, default=1)
    next_value = models.BigIntegerField(default=0)


class ShortURL(models.Model):
    code = models.CharField(max_length=16, unique=True, db_index=True)
    long_url = models.URLField(max_length=2048)
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL
    )
    session_key = models.CharField(max_length=40, null=True, blank=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return self.code

    def short_url(self):
        return f"{settings.SITE_BASE_URL}/{self.code}"

    def is_expired(self):
        if not self.expires_at:
            return False
        from django.utils import timezone

        return timezone.now() >= self.expires_at


class Click(models.Model):
    short_url = models.ForeignKey(ShortURL, on_delete=models.CASCADE, related_name="clicks")
    # Set by the publisher (shortener.views), not the consumer, so replaying the
    # same Kafka event after an at-least-once redelivery hits this unique
    # constraint instead of creating a duplicate row.
    event_id = models.CharField(max_length=36, unique=True, null=True)
    timestamp = models.DateTimeField(db_index=True)
    referrer = models.CharField(max_length=512, blank=True, default="")
    ip_hash = models.CharField(max_length=64, blank=True, default="")
    user_agent = models.CharField(max_length=512, blank=True, default="")

    class Meta:
        ordering = ["-timestamp"]
        indexes = [
            models.Index(fields=["short_url", "timestamp"]),
        ]
