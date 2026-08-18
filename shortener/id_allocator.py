"""Pre-allocated range ID allocation.

Instead of one Postgres round trip per shorten request (the previous
CodeSequence auto-increment approach), each app process claims a block of
IDs from a central counter row in one atomic statement, then hands IDs out
of that block in memory with zero DB round trips until the block is
exhausted.

Claiming is safe under concurrent claims from multiple processes/replicas
because the claim is a single `UPDATE ... RETURNING` against one row —
Postgres takes a row lock for the duration of the UPDATE, so two processes
claiming at the same instant are serialized by the database itself and
walk away with disjoint ranges. No SELECT FOR UPDATE + separate UPDATE is
needed because there's nothing to read before deciding what to write; the
new value is computed by the database (`next_value + block_size`) in the
same statement that returns it.

Trade-off: if a process restarts (or crashes) with unused IDs left in its
current block, those IDs are simply never issued — the block-size default
below wastes at most 100,000 codes per restart. Base62 encoding a
4-character code alone covers 62^4 ≈ 14.8M values, and codes grow to 5+
characters automatically as the counter climbs, so burning a block on
every restart is cheap relative to the address space and not worth
reclaiming.
"""

import threading

from django.conf import settings
from django.db import connection

DEFAULT_BLOCK_SIZE = getattr(settings, "ID_ALLOCATION_BLOCK_SIZE", 100_000)


class RangeAllocator:
    def __init__(self, block_size: int = DEFAULT_BLOCK_SIZE):
        self.block_size = block_size
        self._lock = threading.Lock()
        self._next = 0
        self._end = 0

    def next_id(self) -> int:
        with self._lock:
            if self._next >= self._end:
                self._claim_new_range()
            value = self._next
            self._next += 1
            return value

    def _claim_new_range(self) -> None:
        new_end = self._try_claim()
        if new_end is None:
            # Counter row doesn't exist yet (first boot against a fresh
            # DB) — create it once, then retry. Every subsequent claim in
            # this process's lifetime (once per exhausted block, not once
            # per request) takes the single-statement fast path above.
            self._ensure_counter_row()
            new_end = self._try_claim()
        self._end = new_end
        self._next = new_end - self.block_size

    def _try_claim(self):
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE shortener_idcounter
                SET next_value = next_value + %s
                WHERE id = 1
                RETURNING next_value
                """,
                [self.block_size],
            )
            row = cursor.fetchone()
        return row[0] if row else None

    @staticmethod
    def _ensure_counter_row() -> None:
        from .models import IdCounter

        IdCounter.objects.get_or_create(pk=1, defaults={"next_value": 0})


# One allocator per process (per gunicorn worker / per `runserver`
# process), holding its currently-claimed range in memory.
_allocator = RangeAllocator()


def next_code_id() -> int:
    return _allocator.next_id()
