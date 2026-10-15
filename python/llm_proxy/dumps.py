"""Request/response dumps, stats.jsonl and stall.log writers. Mirrors the
Lua dump() and _M.log_request(). Write failures never break the proxy path
— they are only logged."""
from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("llm_proxy.dump")


def _json_line(obj: dict) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"),
                      sort_keys=True)


def _floor(x: float) -> int:
    return math.floor(x + 0.5)


def _utc_str(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stat_num(v) -> int:
    """Token count for the stats log: any missing/invalid value becomes 0."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return math.floor(v + 0.5)
    return 0


def dump_ts() -> int:
    """Millisecond-precision Unix timestamp used for dump file names."""
    return math.floor(__import__("time").time() * 1000)


def dump(log_dir: Path, ts: int, kind: str, data: bytes | str | None) -> None:
    if data is None:
        return
    try:
        path = Path(log_dir) / f"{ts}.{kind}.json"
        mode = "wb" if isinstance(data, (bytes, bytearray)) else "w"
        with open(path, mode) as f:
            f.write(data)
    except OSError as err:
        log.error("failed to write %s dump: %s", kind, err)


def write_stats(cfg, ctx: dict) -> None:
    """Append one stats.jsonl line for requests actually forwarded to the
    upstream (ctx has _sent_at). Mirrors the Lua log_request stats block."""
    if not ctx.get("_sent_at"):
        return
    ended = ctx.get("_ended", __import__("time").time())
    sent_at = ctx["_sent_at"]
    usage = ctx.get("_resp_usage")
    if not isinstance(usage, dict):
        usage = {}
    model = ctx.get("_resp_model")
    if not model:
        model = ctx.get("_req_model")
    if not model:
        model = "unknown"

    stall_s = _stall_seconds(ctx, ended)

    entry = {
        "model": model,
        "prompt_tokens": stat_num(usage.get("prompt_tokens")),
        "total_tokens": stat_num(usage.get("total_tokens")),
        "completion_tokens": stat_num(usage.get("completion_tokens")),
        "status": ctx.get("_upstream_status") or ctx.get("_final_status", 0),
        "stall_seconds": stall_s,
        "start_time_unix": _floor(sent_at * 1000),
        "start_time": _utc_str(math.floor(sent_at + 0.5)),
        "duration_ms": _floor(((ctx.get("_upstream_done") or ended) - sent_at) * 1000),
    }
    for key in ("_toolname_remap", "_toolname_strip", "_toolname_resend",
                "_upstream_retries"):
        val = ctx.get(key)
        if val and val > 0:
            entry[key.lstrip("_")] = val
    _append_jsonl(cfg.stats_log, entry)


def write_stall(cfg, ctx: dict) -> None:
    """Append the stall.log line (only when temperature checks are enabled).
    Mirrors the Lua log_request stall block."""
    if not cfg.temp_checks_enabled or not cfg.temp_check_stall_log:
        return
    ended = ctx.get("_ended", __import__("time").time())
    start = ctx.get("_start", ended)
    entry = {
        "time": _utc_str(ended),
        "ts": start,
        "uri": ctx.get("_uri", ""),
        "status": ctx.get("_final_status", 0),
        "aborted": ctx.get("_aborted", False) is True,
        "total_s": _floor((ended - start) * 1000) / 1000,
        "stall_s": _stall_seconds(ctx, ended),
    }
    if ctx.get("_stall_start"):
        entry["stall_start"] = ctx["_stall_start"]
        entry["stall_end"] = ctx.get("_stall_end") or ended
    _append_jsonl(cfg.temp_check_stall_log, entry)


def _stall_seconds(ctx: dict, ended: float) -> float:
    if ctx.get("_stall_start"):
        return _floor(((ctx.get("_stall_end") or ended) - ctx["_stall_start"]) * 1000) / 1000
    return 0.0


def _append_jsonl(path: Path, entry: dict) -> None:
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(_json_line(entry) + "\n")
    except OSError as err:
        log.error("failed to open stats log %s: %s", path, err)
