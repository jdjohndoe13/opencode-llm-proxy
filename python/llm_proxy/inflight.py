"""In-flight request registry for the dashboard.

Records every /v1/completions request from entry to exit so the dashboard
can show what is currently waiting (paused / stalled / upstream). Purely
in-memory; the dict is the single source of truth and each record is a
plain dict so list_inflight() can cheaply copy it."""
from __future__ import annotations

import time


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
        }
        self._records[rec_id] = rec
        return rec

    def remove(self, rec_id: str) -> None:
        self._records.pop(rec_id, None)

    def set_state(self, rec: dict, state: str) -> None:
        if rec is None:
            return
        rec["state"] = state
        rec["updated_at"] = time.time()

    def list_inflight(self) -> list[dict]:
        """Snapshot of in-flight requests, oldest first, with age_seconds."""
        now = time.time()
        out = []
        for rec in self._records.values():
            d = dict(rec)
            d["age_seconds"] = round(now - rec["started_at"], 3)
            out.append(d)
        out.sort(key=lambda r: r["started_at"])
        return out
