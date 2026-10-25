"""Per-process admission control: concurrency slots and per-institution rate.

These are per-instance budgets. With several replicas behind the platform the
effective totals are N x the configured values; a global budget belongs in the
platform or gateway. Excess calls are refused immediately instead of queueing.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from .config import Limits
from .errors import BUSY, RATE_LIMITED, BizDbError


@dataclass
class _Bucket:
    tokens: float
    updated: float


class CallGate:
    """Thread-safe admission for tool calls, keyed by normalized institution code."""

    def __init__(self, limits: Limits, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._limits = limits
        self._clock = clock
        self._lock = threading.Lock()
        self._total = 0
        self._per_institution: dict[str, int] = {}
        self._per_kind: dict[str, int] = {"export": 0, "import": 0}
        self._buckets: dict[str, _Bucket] = {}

    @contextmanager
    def admit(self, institution_code: str, kind: str = "read") -> Iterator[None]:
        """Hold one slot for the duration of a call; ``kind`` is read, export or import."""
        with self._lock:
            self._take_token(institution_code)
            self._reserve(institution_code, kind)
        try:
            yield
        finally:
            with self._lock:
                self._release(institution_code, kind)

    def _take_token(self, code: str) -> None:
        rate = self._limits.rate
        now = self._clock()
        bucket = self._buckets.get(code)
        if bucket is None:
            bucket = self._buckets[code] = _Bucket(tokens=float(rate.burst), updated=now)
        refill = (now - bucket.updated) * rate.per_minute / 60.0
        bucket.tokens = min(float(rate.burst), bucket.tokens + refill)
        bucket.updated = now
        if bucket.tokens < 1.0:
            raise BizDbError(RATE_LIMITED, "too many calls for this institution; retry later")
        bucket.tokens -= 1.0

    def _reserve(self, code: str, kind: str) -> None:
        concurrency = self._limits.concurrency
        if self._total >= concurrency.total:
            raise BizDbError(BUSY, "server is at its concurrent call limit; retry later")
        if self._per_institution.get(code, 0) >= concurrency.per_institution:
            raise BizDbError(BUSY, "too many concurrent calls for this institution; retry later")
        kind_limit = {"export": concurrency.export, "import": concurrency.import_}.get(kind)
        if kind_limit is not None and self._per_kind[kind] >= kind_limit:
            raise BizDbError(BUSY, f"another {kind} is running; retry later")
        self._total += 1
        self._per_institution[code] = self._per_institution.get(code, 0) + 1
        if kind in self._per_kind:
            self._per_kind[kind] += 1

    def _release(self, code: str, kind: str) -> None:
        self._total -= 1
        remaining = self._per_institution.get(code, 1) - 1
        if remaining:
            self._per_institution[code] = remaining
        else:
            self._per_institution.pop(code, None)
        if kind in self._per_kind:
            self._per_kind[kind] -= 1
