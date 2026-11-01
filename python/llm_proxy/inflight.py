"""In-flight request registry for the dashboard.

Records every /v1/completions request from entry to exit so the dashboard
can show what is currently waiting (paused / stalled / queued / upstream)
and how long each request has spent held vs being processed.
Purely in-memory; the dict is the single source of truth and each record is
a plain dict so list_inflight() can cheaply copy it."""
from __future__ import annotations

import time

# States in which the request is NOT being worked on by the LLM server: the
# manual pause gate, the temperature guard's cool-down stall, and the
# concurrency limiter's queue. All three are "held" from the client's view.
HELD_STATES = ("paused", "queued", "stalled")


class InFlight:
    def __init__(self):
        self._records: dict[str, dict] = {}

    def add(self, rec_id: str, *, model, path: str, stream: bool) -> dict:
        now = time.time()
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
