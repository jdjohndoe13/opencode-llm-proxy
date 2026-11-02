"""In-flight request registry for the dashboard.

Records every /v1/completions request from entry to exit so the dashboard
can show what is currently waiting (paused / stalled / queued / upstream)
and how long each request has spent held vs being processed.
Purely in-memory; the dict is the single source of truth and each record is
a plain dict so list_inflight() can cheaply copy it."""
from __future__ import annotations

import asyncio
import logging
import time

log = logging.getLogger("llm_proxy.inflight")

# How long drop_all() waits for the cancelled handlers to unwind before
# answering anyway (the unwinding is bounded so a wedged handler can never
# hang POST /api/dashboard/drop).
DROP_WAIT = 1.0

# States in which the request is NOT being worked on by the LLM server: the
# manual pause gate, the temperature guard's cool-down stall, and the
# concurrency limiter's queue. All three are "held" from the client's view.
HELD_STATES = ("paused", "queued", "stalled")


class InFlight:
    def __init__(self):
        self._records: dict[str, dict] = {}
        # Monotonic per-registry counter: the ms timestamp alone is not
        # unique (see add()).
        self._seq = 0
        # Private side-map of the handles drop_all() needs to sever a
        # request: {"ctx": ..., "transport": ..., "task": ...}. Deliberately
        # NOT inside the record dict: list_inflight() copies records into
        # JSON for the dashboard.
        self._kill: dict[str, dict] = {}

    def add(self, rec_id: str, *, model, path: str, stream: bool) -> dict:
        now = time.time()
        # The caller's id is a millisecond timestamp — two requests in the
        # same millisecond would overwrite each other's record (and kill
        # entry). Suffix a monotonic counter and return the record so the
        # caller uses this id (register_kill / remove) everywhere.
        self._seq += 1
        rec_id = f"{rec_id}-{self._seq}"
        rec = {
            "id": rec_id,
            "model": model if isinstance(model, str) else None,
            "path": path,
            "stream": bool(stream),
            "started_at": now,
            "state": "waiting",
            "updated_at": now,
            # accumulated seconds in each phase; the time still in the
            # current state is added on read (see list_inflight)
            "held_seconds": 0.0,
            "upstream_seconds": 0.0,
        }
        self._records[rec_id] = rec
        return rec

    def remove(self, rec_id: str) -> None:
        self._records.pop(rec_id, None)
        self._kill.pop(rec_id, None)

    def register_kill(self, rec_id: str, ctx: dict, transport, task) -> None:
        """Remember the handles needed to sever this request (see drop_all).

        Called by the handler as soon as the record exists. drop_all()
        tolerates a missing handle (e.g. request.transport is None once the
        connection is gone), so there is nothing to unregister here."""
        self._kill[rec_id] = {
            "ctx": ctx,
            "transport": transport,
            "task": task,
        }

    async def drop_all(self) -> int:
        """Sever every in-flight request right now: no forwarding, no
        response, no flush — the client sees a connection error.

        Per request, in this order:
        1. ctx["_aborted"] = True — the wait loops (pause gate / temp guard /
           limiter) observe it on their next tick and unwind even if the
           cancellation is lost.
        2. task.cancel() — the handler task. aiohttp runs the handler on a
           per-request task (it is a different task per request even on one
           keep-alive connection), so this is the precise signal; the
           handler unwinds through CancelledError and stats_middleware's
           finally releases the slot, removes the record and writes the
           stats line.
        3. transport.abort() — close the socket without flushing, so the
           peer gets an RST instead of a well-formed HTTP answer.

        The stored task is the handler's own task: measured on aiohttp
        3.14, asyncio.current_task() inside the handler is a PER-REQUEST
        task (two keep-alive requests on one connection get two tasks), so
        cancelling it hits exactly one request. The signal is still deduped
        by id(task) — if a runner ever hands the handler the per-connection
        task instead, one task must be cancelled once, not once per
        request, and the second entry's transport.abort() + _aborted flag
        still sever its own request.

        Not fire-and-forget: after signalling, the cancelled handler tasks
        are awaited (bounded) so the registry is empty by the time the
        response is sent — `dropped:n` and the post-drop /state agree.

        One bad entry must not stop the rest, so each is guarded. Returns
        how many requests were dropped."""
        tasks = []
        seen = set()
        n = 0
        for rec_id, h in list(self._kill.items()):
            try:
                ctx = h.get("ctx")
                if ctx is not None:
                    ctx["_aborted"] = True
                task = h.get("task")
                if task is not None and id(task) not in seen:
                    seen.add(id(task))
                    if not task.done():
                        task.cancel()
                    if not task.done():
                        tasks.append(task)
                transport = h.get("transport")
                if transport is not None:
                    transport.abort()
            except Exception:
                log.exception("dropping in-flight request %s failed", rec_id)
            finally:
                # Dropped either way; the registry record itself goes when
                # the handler's finally runs.
                self._kill.pop(rec_id, None)
                n += 1
        if tasks:
            # Let the handlers unwind (stats / slot release / record
            # removal) before answering, so the dashboard's next /state
            # already reflects the drop. Bounded so a wedged handler can
            # never hang the endpoint.
            await asyncio.wait(tasks, timeout=DROP_WAIT)
        return n

    def set_state(self, rec: dict, state: str) -> None:
        """Move a request to a new state, closing out the previous one.

        Every state change funnels through here, so the per-phase totals are
        just the sum of the intervals each state was open — no separate
        bookkeeping is needed at the call sites."""
        if rec is None:
            return
        now = time.time()
        prev = rec.get("state")
        if prev != state:
            spent = now - rec["updated_at"]
            if spent > 0:
                if prev in HELD_STATES:
                    rec["held_seconds"] = rec.get("held_seconds", 0.0) + spent
                elif prev == "upstream":
                    rec["upstream_seconds"] = (
                        rec.get("upstream_seconds", 0.0) + spent)
        rec["state"] = state
        rec["updated_at"] = now

    def list_inflight(self) -> list[dict]:
        """Snapshot of in-flight requests, oldest first, with age_seconds.

        held_seconds / upstream_seconds include the time the request has
        been sitting in its CURRENT state, so the split always adds up to
        the request's age (minus the sub-tick transits between gates)."""
        now = time.time()
        out = []
        for rec in self._records.values():
            d = dict(rec)
            held = rec.get("held_seconds", 0.0)
            up = rec.get("upstream_seconds", 0.0)
            open_for = now - rec["updated_at"]
            if rec["state"] in HELD_STATES:
                held += open_for
            elif rec["state"] == "upstream":
                up += open_for
            d["held_seconds"] = round(held, 3)
            d["upstream_seconds"] = round(up, 3)
            d["age_seconds"] = round(now - rec["started_at"], 3)
            out.append(d)
        out.sort(key=lambda r: r["started_at"])
        return out
