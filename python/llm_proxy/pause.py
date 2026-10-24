"""Manual pause/resume gate.

In-memory only: a restart always boots running (unpaused). Mirrors the
structure of tempguard.wait_for_cool_down — a poll loop with a ~0.25s tick
that awaits on_wait() each pass so streaming clients keep getting the
deferred SSE handshake and fake empty heartbeat chunks while the proxy is
paused, and never touches the upstream while held.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque

log = logging.getLogger("llm_proxy.pause")

TICK = 0.25
EVENTS_MAX = 200


class PauseGate:
    def __init__(self):
        self.paused: bool = False
        self.paused_at: float | None = None
        self._events: deque[dict] = deque(maxlen=EVENTS_MAX)
        self._started = time.time()

    # -- state ----------------------------------------------------------

    @property
    def is_paused(self) -> bool:
        return self.paused

    def pause(self) -> None:
        if self.paused:
            return
        self.paused = True
        self.paused_at = time.time()
        self._record("pause", self.paused_at)
        log.info("proxy paused")

    def resume(self) -> None:
        if not self.paused:
            return
        now = time.time()
        self.paused = False
        self._record("resume", now)
        self.paused_at = None
        log.info("proxy resumed")

    def _record(self, action: str, ts: float) -> None:
        self._events.append({"ts": ts, "action": action})

    def events(self) -> list[dict]:
        """Recent pause/resume events, newest first."""
        return list(reversed(self._events))

    def uptime_s(self) -> float:
        return round(time.time() - self._started, 3)

    # -- the gate -------------------------------------------------------

    async def wait_until_resumed(self, ctx: dict, on_wait=None) -> bool:
        """Block while paused. Returns True when resumed, False when the
        client aborted while waiting (ctx["_aborted"], same signal as
        tempguard). on_wait is awaited every tick so stream:true clients
        keep the identical deferred handshake + heartbeat behavior; for
        non-stream clients on_wait is a no-op and the request simply hangs
        with no reply bytes until resumed or the client's own timeout
        kills it. ctx keys: _paused_at, _paused_seconds."""
        if not self.paused:
            return True
        ctx.setdefault("_paused_at", time.time())
        while self.paused:
            if ctx.get("_aborted"):
                return False
            if on_wait is not None:
                await on_wait()
            await asyncio.sleep(TICK)
        if ctx.get("_paused_at"):
            ctx["_paused_seconds"] = (
                round((time.time() - ctx["_paused_at"]) * 1000) / 1000)
        return True
