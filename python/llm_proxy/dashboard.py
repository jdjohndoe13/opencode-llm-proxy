"""Dashboard API + static UI mount.

Routes (registered BEFORE the catch-all passthrough so they never reach
the backend):

  GET  /api/dashboard/state     pause state + in-flight + keepalive + events
  POST /api/dashboard/pause     pause the proxy (in-flight requests keep going)
  POST /api/dashboard/resume    resume the proxy
  GET  /api/dashboard/history   paginated stats.jsonl tail, newest first
  POST /api/dashboard/tempguard             on/off for the temperature guard
  POST /api/dashboard/tempguard/thresholds  edit the hysteresis pair
  GET  /api/dashboard/healthz   {ok:true} — lets the UI tell proxy vs backend
  GET  /dashboard               the built static UI (python/llm_proxy/static)
  GET  /dashboard/assets/...    static assets

Plain JSON, no auth, CORS wide open for the dev server.
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from pathlib import Path

from aiohttp import web

from .pause import PauseGate

log = logging.getLogger("llm_proxy.dashboard")

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}

MAX_PAGE_SIZE = 500
CHUNK = 1 << 16


def _json(data: dict, status: int = 200) -> web.Response:
    return web.json_response(data, status=status, headers=CORS)


# ---------------------------------------------------------------- history

def _count_lines(path: Path) -> int:
    """Number of complete (newline-terminated) lines, chunked so the file
    is never held in memory."""
    n = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                return n
            n += chunk.count(b"\n")


def _tail_lines(path: Path, need: int) -> tuple[list[bytes], bool]:
    """The last `need` complete lines of `path`, newest first, read as a
    tail window that grows in chunks. Returns (lines, at_start_of_file).

    A trailing partial line (no final newline, e.g. a write interrupted by
    a crash) is skipped, as is any line that cannot be parsed later. The
    whole file is never loaded: only the window is held in memory."""
    if need <= 0:
        return [], False
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        window = 0
        while True:
            window = min(size, window + CHUNK)
            f.seek(size - window)
            data = f.read(window)
            at_start = window == size
            # Every split element is a newline-terminated line except the
            # last one (the phantom after a final newline, or the trailing
            # partial line) and the first one when the window starts
            # mid-line — both dropped.
            parts = data.split(b"\n")[:-1]
            if not at_start:
                parts = parts[1:]
            if len(parts) >= need or at_start:
                return list(reversed(parts)), at_start
            if window >= 64 * 1024 * 1024:  # pathological single line
                return list(reversed(parts)), False


def read_history(path: Path, page: int = 1, size: int = 20) -> dict:
    """One page of stats.jsonl, newest first. Missing file, trailing
    partial lines and unparseable lines are all tolerated. The page*size
    window is capped so a bogus deep page cannot force a full-file scan."""
    page = max(1, page)
    size = max(1, min(size, MAX_PAGE_SIZE))
    if page * size > 10_000:
        page = max(1, 10_000 // size)
    total = 0
    entries: list[dict] = []
    try:
        if path.exists():
            lines, at_start = _tail_lines(path, page * size)
            if at_start:
                total = len(lines)
            else:
                total = _count_lines(path)
            window = lines[(page - 1) * size: page * size]
            for raw in window:
                try:
                    obj = json.loads(raw.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    continue
                if isinstance(obj, dict):
                    entries.append(obj)
    except OSError as err:
        log.error("failed to read stats history %s: %s", path, err)
    return {
        "total": total,
        "page": page,
        "size": size,
        "pages": math.ceil(total / size) if total else 0,
        "entries": entries,
    }


# ---------------------------------------------------------------- handlers

def _int_param(request: web.Request, name: str, default: int) -> int:
    raw = request.query.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


async def _preflight(request: web.Request) -> web.Response:
    return web.Response(status=204, headers=CORS)


def _api(fn):
    """CORS preflight guard: an OPTIONS request must never trigger the
    actual action (pause/resume), it only gets the CORS headers back."""
    async def handler(request: web.Request) -> web.Response:
        if request.method == "OPTIONS":
            return web.Response(status=204, headers=CORS)
        return await fn(request)
    return handler


async def state(request: web.Request) -> web.Response:
    gate: PauseGate = request.app["pause_gate"]
    cfg = request.app["cfg"]
    return _json({
        "paused": gate.is_paused,
        "paused_at": gate.paused_at,
        "uptime_s": gate.uptime_s(),
        "in_flight": request.app["inflight"].list_inflight(),
        "keepalive": {
            "delay": cfg.keepalive_delay,
            "interval": cfg.keepalive_interval,
        },
        "events": gate.events(),
        "temp_guard": request.app["temp_guard"].status(),
    })


async def pause(request: web.Request) -> web.Response:
    gate: PauseGate = request.app["pause_gate"]
    gate.pause()
    return _json({"ok": True, "paused": True})


async def resume(request: web.Request) -> web.Response:
    gate: PauseGate = request.app["pause_gate"]
    gate.resume()
    return _json({"ok": True, "paused": False})


async def history(request: web.Request) -> web.Response:
    cfg = request.app["cfg"]
    return _json(read_history(
        cfg.stats_log,
        page=_int_param(request, "page", 1),
        size=_int_param(request, "size", 20)))


async def healthz(request: web.Request) -> web.Response:
    return _json({"ok": True, "uptime_s": request.app["pause_gate"].uptime_s()})


async def tempguard(request: web.Request) -> web.Response:
    """POST /api/dashboard/tempguard  {"enabled": true|false}.

    Runtime on/off switch for the GPU temperature guard (in-memory, resets
    to on at restart). Without TEMP_CHECK_ENABLED the guard is inactive
    regardless of this switch; the state endpoint reports both bits."""
    guard = request.app["temp_guard"]
    try:
        body = await request.json()
    except Exception:
        body = None
    if not isinstance(body, dict) or not isinstance(body.get("enabled"), bool):
        return _json({"ok": False, "error": "expected {\"enabled\": true|false}"},
                     status=400)
    guard.set_user_enabled(body["enabled"])
    return _json({"ok": True, "enabled": guard.user_enabled})


async def tempguard_thresholds(request: web.Request) -> web.Response:
    """POST /api/dashboard/tempguard/thresholds
    {"stall_above": 74, "resume_below": 60}   (either key may be omitted)
    {"reset": true}                            (back to the .env values)

    Runtime edit of the hysteresis pair — the same in-memory semantics as
    the on/off switch, so a restart re-reads TEMP_CHECK_* from .env. The
    pair is validated as a whole (see validate_thresholds) and a rejected
    edit leaves the live values untouched. Requires TEMP_CHECK_ENABLED:
    with the guard disarmed by config there is nothing to tune."""
    guard = request.app["temp_guard"]
    if not guard.cfg.temp_checks_enabled:
        return _json({"ok": False,
                      "error": "temperature guard is disabled by config"},
                     status=400)
    try:
        body = await request.json()
    except Exception:
        body = None
    if not isinstance(body, dict):
        return _json({"ok": False,
                      "error": "expected {\"stall_above\": n, \"resume_below\": n}"
                               " or {\"reset\": true}"},
                     status=400)
    if body.get("reset") is True:
        guard.reset_thresholds()
    else:
        unknown = set(body) - {"stall_above", "resume_below"}
        if unknown:
            return _json({"ok": False,
                          "error": f"unexpected keys: {', '.join(sorted(unknown))}"},
                         status=400)
        if not {"stall_above", "resume_below"} & set(body):
            return _json({"ok": False,
                          "error": "expected \"stall_above\" and/or \"resume_below\""},
                         status=400)
        for key in ("stall_above", "resume_below"):
            if key in body and not isinstance(body[key], (int, float)):
                return _json({"ok": False,
                              "error": f"{key} must be a number"},
                             status=400)
        applied, err = guard.set_thresholds(
            stall_above=body.get("stall_above"),
            resume_below=body.get("resume_below"))
        if not applied:
            return _json({"ok": False, "error": err}, status=400)
    return _json({"ok": True,
                  "stall_above": guard.stall_above,
                  "resume_below": guard.resume_below})


# ---------------------------------------------------------------- static UI

def _content_type(path: Path) -> str:
    return {
        ".html": "text/html; charset=utf-8",
        ".js": "text/javascript; charset=utf-8",
        ".mjs": "text/javascript; charset=utf-8",
        ".css": "text/css; charset=utf-8",
        ".txt": "text/plain; charset=utf-8",
        ".json": "application/json",
        ".svg": "image/svg+xml",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".ico": "image/x-icon",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
        ".map": "application/json",
    }.get(path.suffix.lower(), "application/octet-stream")


def _serve_file(root: Path, rel: str) -> web.Response:
    """Serve root/rel with a path-traversal guard."""
    if rel:
        candidate = (root / rel).resolve()
        try:
            root_res = root.resolve()
            if candidate.is_file() and root_res in candidate.parents:
                return web.Response(
                    body=candidate.read_bytes(),
                    headers={"Content-Type": _content_type(candidate)})
        except OSError:
            pass
    return web.Response(status=404, text="not found")


def _index(request: web.Request) -> web.Response:
    root: Path = request.app["static_dir"]
    idx = root / "index.html"
    if idx.is_file():
        return web.Response(body=idx.read_bytes(),
                            headers={"Content-Type": "text/html; charset=utf-8"})
    return web.Response(status=404, text="dashboard not built")


async def dashboard_root(request: web.Request) -> web.Response:
    return _index(request)


async def dashboard_asset(request: web.Request) -> web.Response:
    return _serve_file(request.app["static_dir"], request.match_info.get("tail", ""))


async def dashboard_static(request: web.Request) -> web.Response:
    """GET /static/<tail> — the same directory under a stable prefix."""
    return _serve_file(request.app["static_dir"], request.match_info.get("tail", ""))


def register(app: web.Application) -> None:
    """Mount the dashboard API + static UI. Must be called before the
    catch-all passthrough route is added."""
    app["pause_gate"] = PauseGate()
    from .inflight import InFlight

    app["inflight"] = InFlight()
    app["static_dir"] = Path(__file__).parent / "static"

    r = app.router
    r.add_route("*", "/api/dashboard/state", _api(state))
    r.add_route("*", "/api/dashboard/pause", _api(pause))
    r.add_route("*", "/api/dashboard/resume", _api(resume))
    r.add_route("*", "/api/dashboard/history", _api(history))
    r.add_route("*", "/api/dashboard/tempguard", _api(tempguard))
    r.add_route("*", "/api/dashboard/tempguard/thresholds",
                _api(tempguard_thresholds))
    r.add_route("*", "/api/dashboard/healthz", _api(healthz))
    r.add_route("*", "/api/dashboard", _preflight)
    r.add_route("*", "/api/dashboard/{tail:.*}", _preflight)
    r.add_route("*", "/dashboard", dashboard_root)
    r.add_route("*", "/dashboard/{tail:.*}", dashboard_asset)
    r.add_route("*", "/static/{tail:.*}", dashboard_static)
