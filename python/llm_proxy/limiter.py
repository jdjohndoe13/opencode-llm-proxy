"""Concurrent-request limiter: a FIFO admission semaphore for the upstream.

At most `limit` requests may be at the LLM server at once (0 = unlimited,
the default). A request that arrives while the pool is full is held in
arrival order and released when a running request finishes — one departure
admits exactly one waiting request, oldest first.

Same in-memory semantics as the pause gate and the temperature guard's
runtime controls: a restart re-reads CONCURRENCY_LIMIT from .env, and the
dashboard can edit the live value.

The wait loop mirrors pause.wait_until_resumed and tempguard.wait_for_cool_down:
a ~0.25s tick that awaits on_wait() each pass, so streaming clients keep the
deferred SSE handshake + heartbeat while queued and never touch the upstream
until they hold a slot. ctx["_aborted"] (set by stats_middleware on client
disconnect) unwinds the wait instead of hanging forever.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque

from .config import Config

log = logging.getLogger("llm_proxy.limit")

TICK = 0.25

# Upper bound for a runtime edit: a sane ceiling for "how many requests may
# hit the LLM server at once", high enough that nobody has to work around it
# and low enough that a typo is still caught.
LIMIT_MAX = 10000


def validate_limit(limit) -> str | None:
    """None when the limit is usable, otherwise a one-line explanation for
    the dashboard (returned to it as a 400)."""
    if not isinstance(limit, (int, float)) or isinstance(limit, bool) \
            or not float(limit).is_integer():
        return "limit must be a whole number of requests"
    if not 0 <= limit <= LIMIT_MAX:
        return f"limit must be between 0 and {LIMIT_MAX} (0 = unlimited)"
    return None


class ConcurrencyLimiter:
    def __init__(self, cfg: Config):
        self.limit: int = cfg.concurrency_limit
        self.default_limit: int = cfg.concurrency_limit
        self._active = 0
        self._queue: deque[asyncio.Future] = deque()

    # -- state ----------------------------------------------------------

    def status(self) -> dict:
        """Snapshot for the dashboard state endpoint."""
        return {
            "limit": self.limit,
            "active": self._active,
            "queued": len(self._queue),
            "default_limit": self.default_limit,
        }

    def set_limit(self, limit: int) -> tuple[bool, str | None]:
        """Apply a runtime limit edit. Raising the limit immediately admits
        as many queued requests as the new capacity allows; lowering it never
        disturbs requests already holding a slot."""
        err = validate_limit(limit)
        if err is not None:
            log.warning("concurrency limit rejected: %s", err)
            return False, err
        self.limit = int(limit)
        log.info("concurrency limit set to %s via dashboard",
                 "unlimited" if self.limit == 0 else self.limit)
        self._drain()
        return True, None

    def reset_limit(self) -> None:
        """Restore the .env value (the dashboard's 'reset' button)."""
        self.limit = self.default_limit
        log.info("concurrency limit reset to .env default (%s)",
                 "unlimited" if self.limit == 0 else self.limit)
        self._drain()

    # -- the gate -------------------------------------------------------

    async def acquire(self, ctx: dict, on_wait=None) -> bool:
        """Wait for a free slot. Returns True when the caller holds a slot
        and MUST call release() exactly once; False when the client aborted
        while queued (no slot was taken, so nothing to release).

        A slot is taken only for the upstream exchange, so a request held by
        the pause gate or the temperature guard does not consume one. ctx
        keys set: _slot_held, _queued_at, _queued_seconds, _queued."""
        # Fast path: nothing queued ahead of us and capacity free. Checking
        # the queue first is what keeps the order FIFO — a request that
        # arrives while anyone is waiting must queue too, even if a slot
        # just came free, or the queue could be jumped indefinitely.
        if not self._queue and (self.limit == 0 or self._active < self.limit):
            self._active += 1
            ctx["_slot_held"] = True
            return True

        fut = asyncio.get_running_loop().create_future()
        self._queue.append(fut)
        ctx["_queued_at"] = time.time()
        log.info("concurrency limit %s reached (%d active, %d queued), "
                 "holding request", self.limit, self._active, len(self._queue))
        granted = False
        try:
            while not fut.done():
                if ctx.get("_aborted"):
                    return False
                if on_wait is not None:
                    await on_wait()
                try:
                    # shield: the tick timeout must not cancel our grant.
                    await asyncio.wait_for(asyncio.shield(fut), TICK)
                except asyncio.TimeoutError:
                    pass
            fut.result()
            # The slot is ours the moment the grant lands: record it (and
            # the queue time) before anything else can be cancelled, so
            # stats_middleware's finally is guaranteed to release it even if
            # the CancelledError arrives right here.
            ctx["_slot_held"] = True
            granted = True
            if ctx.get("_queued_at") and not ctx.get("_queued_seconds"):
                ctx["_queued_seconds"] = (
                    round((time.time() - ctx["_queued_at"]) * 1000) / 1000)
            return True
        finally:
            # The queue must never keep a dead reservation, and a slot that
            # _drain reserved for us must go back if we are leaving without
            # it — the handler is cancelled outright on client disconnect
            # (handler_cancellation), so the grant can land at the same
            # moment the CancelledError arrives.
            try:
                self._queue.remove(fut)
            except ValueError:
                pass
            if not granted:
                if not fut.done():
                    fut.cancel()
                elif not fut.cancelled():
                    self.release()

    def release(self) -> None:
        """Give up the caller's slot; admits the oldest waiter if one exists."""
        if self._active > 0:
            self._active -= 1
        self._drain()

    def _drain(self) -> None:
        """Hand free capacity to queued waiters, oldest first."""
        while self._queue and (self.limit == 0 or self._active < self.limit):
            fut = self._queue.popleft()
            if fut.done():       # cancelled meanwhile: it already left
                continue
            self._active += 1    # reserve the slot for this waiter
            fut.set_result(True)
