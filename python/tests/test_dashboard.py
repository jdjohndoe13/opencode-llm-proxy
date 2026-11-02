"""E2E tests for the pause gate + dashboard API (same style as
test_server_e2e.py: ephemeral ports only, the real proxy is never touched).
"""
import asyncio
import json
import time

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from llm_proxy.config import load
from llm_proxy.dashboard import read_history
from llm_proxy.server import create_app

CHAT = {
    "id": "chatcmpl-1", "object": "chat.completion", "created": 1,
    "model": "m",
    "choices": [{"index": 0, "message": {"role": "assistant",
                                         "content": "hello world",
                                         "reasoning": ""},
                 "finish_reason": "stop"}],
    "usage": {"prompt_tokens": 3, "completion_tokens": 2,
              "total_tokens": 5},
}


def _cfg(tmp_path, backend, **over):
    env = {
        "LLM_BACKEND": backend, "KEEPALIVE_DELAY": "1",
        "KEEPALIVE_INTERVAL": "1", "UPSTREAM_TIMEOUT": "5",
        "UPSTREAM_RETRIES": "1", "LOG_DIR": str(tmp_path),
    }
    env.update(over)
    return load(env)


async def _start(tmp_path, handler=None, over=None, hc=False):
    """Proxy + mock backend that records call count. Returns (backend,
    proxy, calls). hc=True mirrors production's handler_cancellation."""
    calls = {"n": 0}

    async def chat(request):
        calls["n"] += 1
        if handler is not None:
            return await handler(request)
        return web.json_response(CHAT)

    backend_app = web.Application()
    backend_app.router.add_post("/v1/chat/completions", chat)
    backend = TestServer(backend_app)
    await backend.start_server()
    cfg = _cfg(tmp_path, f"127.0.0.1:{backend.port}", **(over or {}))
    runner_kw = {"handler_cancellation": True} if hc else {}
    proxy = TestClient(TestServer(create_app(cfg), **runner_kw))
    await proxy.start_server()
    return backend, proxy, calls


async def _pause(proxy):
    async with proxy.post("/api/dashboard/pause") as r:
        assert r.status == 200
        assert await r.json() == {"ok": True, "paused": True}


async def _resume(proxy):
    async with proxy.post("/api/dashboard/resume") as r:
        assert r.status == 200
        assert await r.json() == {"ok": True, "paused": False}


async def _drop(proxy):
    async with proxy.post("/api/dashboard/drop") as r:
        assert r.status == 200
        assert r.headers.get("Access-Control-Allow-Origin") == "*"
        return await r.json()


async def _state(proxy):
    async with proxy.get("/api/dashboard/state") as r:
        assert r.status == 200
        assert r.headers.get("Access-Control-Allow-Origin") == "*"
        return await r.json()


async def _gone(coro):
    """Await a request we expect to be severed: the client must see a
    connection error, not a response."""
    try:
        r = await coro
    except (aiohttp.ClientError, OSError):
        return
    assert False, f"dropped request must not answer, got HTTP {r.status}"


async def _collect(resp):
    whole = ""
    async for chunk in resp.content.iter_any():
        whole += chunk.decode()
    return whole


def _content_of(whole):
    out = ""
    for line in whole.splitlines():
        if line.startswith("data: ") and line != "data: [DONE]":
            j = json.loads(line[6:])
            ch = j.get("choices")
            if ch and isinstance(ch[0].get("delta", {}).get("content"), str):
                out += ch[0]["delta"]["content"]
    return out


def _is_heartbeat(line):
    if not line.startswith("data: ") or line == "data: [DONE]":
        return False
    try:
        j = json.loads(line[6:])
    except ValueError:
        return False
    ch = j.get("choices")
    return bool(ch) and ch[0].get("delta") == {}


# ------------------------------------------------------------------ stream

def test_pause_streams_heartbeats_no_upstream_then_resume(tmp_path):
    """stream:true while paused: SSE handshake + empty heartbeats arrive,
    the backend is never touched; resume -> the real answer completes."""
    async def main():
        backend, proxy, calls = await _start(tmp_path)
        try:
            await _pause(proxy)
            loop = asyncio.get_running_loop()
            t0 = loop.time()
            task = asyncio.create_task(
                proxy.post("/v1/chat/completions",
                           json={"model": "m", "messages": [],
                                 "stream": True}))
            async with await asyncio.wait_for(task, 10) as r:
                assert r.status == 200
                assert r.headers["Content-Type"] == "text/event-stream"
                # The deferred handshake commits ~KEEPALIVE_DELAY after the
                # request and the first fake heartbeat arrives with it,
                # while still paused and with NO upstream call.
                first = (await asyncio.wait_for(r.content.read(4096), 5)).decode()
                assert loop.time() - t0 < 4.0
                assert _is_heartbeat(first.splitlines()[0])
                assert calls["n"] == 0

                # Heartbeats keep coming while paused (one more beat).
                beat = (await asyncio.wait_for(r.content.read(4096), 5)).decode()
                assert _is_heartbeat(beat.splitlines()[0])
                assert calls["n"] == 0

                await _resume(proxy)
                rest = await _collect(r)
            whole = first + beat + rest
            assert _content_of(whole) == "hello world"
            assert whole.rstrip().endswith("data: [DONE]")
            assert calls["n"] == 1
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


# ---------------------------------------------------------------- non-stream

def test_pause_nonstream_hangs_then_completes(tmp_path):
    """Non-stream while paused: no response bytes at all; after resume the
    request completes normally."""
    async def main():
        backend, proxy, calls = await _start(tmp_path)
        try:
            await _pause(proxy)
            task = asyncio.create_task(
                proxy.post("/v1/chat/completions",
                           json={"model": "m", "messages": [],
                                 "stream": False}))
            # No response headers arrive while paused (post() only resolves
            # with the headers).
            try:
                await asyncio.wait_for(asyncio.shield(task), 1.5)
                done = True
            except asyncio.TimeoutError:
                done = False
            assert not done, "paused request must not respond"
            assert calls["n"] == 0, "no upstream call while paused"

            # The registry knows about it, in the paused state.
            st = await _state(proxy)
            assert len(st["in_flight"]) == 1
            assert st["in_flight"][0]["state"] == "paused"
            assert st["in_flight"][0]["stream"] is False
            assert st["in_flight"][0]["model"] == "m"
            assert st["in_flight"][0]["age_seconds"] > 0

            await _resume(proxy)
            async with await asyncio.wait_for(task, 10) as r:
                assert r.status == 200
                body = await r.json()
            assert body["object"] == "chat.completion"
            assert calls["n"] == 1
            # Registry cleaned once the request finished.
            st = await _state(proxy)
            assert st["in_flight"] == []
            # paused_seconds recorded in stats.jsonl.
            stats = (tmp_path / "stats.jsonl").read_text(encoding="utf-8")
            entry = json.loads(stats.strip())
            assert entry["paused_seconds"] > 0
            assert entry["status"] == 200
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_client_abort_while_paused(tmp_path):
    """Abort a stream request while paused (handshake already sent): the
    handler unwinds cleanly, the registry is cleaned, the proxy lives."""
    async def main():
        backend, proxy, calls = await _start(tmp_path)
        try:
            await _pause(proxy)
            r = await proxy.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [], "stream": True})
            assert r.status == 200
            # Read one heartbeat, then hang up.
            await r.content.read(1024)
            r.close()
            await asyncio.sleep(0.8)
            st = await _state(proxy)
            assert st["in_flight"] == [], "registry must be cleaned on abort"
            assert calls["n"] == 0
            # Still alive and serving.
            async with proxy.get("/health") as h:
                assert await h.text() == "ok"
            # A fresh request after resume still works.
            await _resume(proxy)
            async with proxy.post(
                    "/v1/chat/completions",
                    json={"model": "m", "stream": True}) as r2:
                whole = await _collect(r2)
            assert _content_of(whole) == "hello world"
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_inflight_before_pause_completes(tmp_path):
    """Requests already in flight when the pause hits finish normally."""
    async def slow(request):
        await asyncio.sleep(0.8)
        return web.json_response(CHAT)

    async def main():
        backend, proxy, calls = await _start(tmp_path, handler=slow)
        try:
            task = asyncio.create_task(
                proxy.post("/v1/chat/completions",
                           json={"model": "m", "stream": False}))
            await asyncio.sleep(0.15)  # request is inside the upstream call
            await _pause(proxy)
            async with await asyncio.wait_for(task, 10) as r:
                assert r.status == 200
                body = await r.json()
            assert body["object"] == "chat.completion"
            assert calls["n"] == 1
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


# ------------------------------------------------------------------- state

def test_state_endpoint_reflects_pause_resume(tmp_path):
    async def main():
        backend, proxy, _ = await _start(tmp_path)
        try:
            st = await _state(proxy)
            assert st["paused"] is False
            assert st["paused_at"] is None
            assert st["in_flight"] == []
            assert st["keepalive"] == {"delay": 1.0, "interval": 1.0}
            # the gate is created during create_app, so a fast first poll
            # can legitimately round to 0.0
            assert st["uptime_s"] >= 0
            assert st["events"] == []

            await _pause(proxy)
            st = await _state(proxy)
            assert st["paused"] is True
            assert isinstance(st["paused_at"], (int, float))
            assert [e["action"] for e in st["events"]] == ["pause"]

            await _resume(proxy)
            st = await _state(proxy)
            assert st["paused"] is False
            assert st["paused_at"] is None
            assert [e["action"] for e in st["events"]] == ["resume", "pause"]

            # healthz distinguishes the proxy from the backend
            async with proxy.get("/api/dashboard/healthz") as r:
                assert r.status == 200
                assert (await r.json())["ok"] is True
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_dashboard_passthrough_untouched(tmp_path):
    """The dashboard routes do not break the catch-all for other paths."""
    async def main():
        backend, proxy, _ = await _start(tmp_path)
        try:
            async with proxy.get("/v1/models") as r:
                assert r.status == 404  # backend has no such route
            # /dashboard serves the placeholder page
            async with proxy.get("/dashboard") as r:
                assert r.status == 200
                text = await r.text()
            assert "dashboard not built yet" in text
            async with proxy.get("/dashboard/../../etc/passwd") as r:
                assert r.status in (400, 404)
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


# ----------------------------------------------------------------- history

def _write_stats(tmp_path, n, partial=False, junk=None):
    lines = [json.dumps({"model": "m", "i": i, "status": 200})
             for i in range(1, n + 1)]
    if junk:
        lines = lines[:junk[0]] + ["not json at all"] + lines[junk[0]:]
    data = "\n".join(lines) + "\n"
    if partial:
        data += '{"model": "m", "i": 999'  # no trailing newline
    (tmp_path / "stats.jsonl").write_text(data, encoding="utf-8")


def test_history_pagination(tmp_path):
    _write_stats(tmp_path, 45, partial=True)
    p1 = read_history(tmp_path / "stats.jsonl", page=1, size=20)
    assert p1["total"] == 45 and p1["pages"] == 3
    assert [e["i"] for e in p1["entries"]] == list(range(45, 25, -1))
    p2 = read_history(tmp_path / "stats.jsonl", page=2, size=20)
    assert [e["i"] for e in p2["entries"]] == list(range(25, 5, -1))
    p3 = read_history(tmp_path / "stats.jsonl", page=3, size=20)
    assert [e["i"] for e in p3["entries"]] == list(range(5, 0, -1))
    p4 = read_history(tmp_path / "stats.jsonl", page=4, size=20)
    assert p4["entries"] == []
    # unparseable lines are skipped, not fatal
    _write_stats(tmp_path, 10, junk=(3,))
    p = read_history(tmp_path / "stats.jsonl", page=1, size=100)
    assert p["total"] == 11 and len(p["entries"]) == 10


def test_history_missing_file(tmp_path):
    p = read_history(tmp_path / "stats.jsonl", page=1, size=20)
    assert p == {"total": 0, "page": 1, "size": 20, "pages": 0,
                 "entries": []}


def test_history_endpoint(tmp_path):
    _write_stats(tmp_path, 25, partial=True)

    async def main():
        backend, proxy, _ = await _start(tmp_path)
        try:
            async with proxy.get(
                    "/api/dashboard/history?page=1&size=10") as r:
                assert r.status == 200
                assert r.headers.get("Access-Control-Allow-Origin") == "*"
                d = await r.json()
            assert d["total"] == 25 and d["pages"] == 3
            assert [e["i"] for e in d["entries"]] == list(range(25, 15, -1))
            async with proxy.get(
                    "/api/dashboard/history?page=2&size=10") as r:
                d = await r.json()
            assert [e["i"] for e in d["entries"]] == list(range(15, 5, -1))
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


# ------------------------------------------------------- temperature guard

TEMP_ENV = {
    "TEMP_CHECK_ENABLED": "1",
    "TEMP_CHECK_STALL_ABOVE": "74",
    "TEMP_CHECK_RESUME_BELOW": "60",
    "TEMP_CHECK_POLL_INTERVAL": "0.1",
    "TEMP_CHECK_CACHE_TTL": "0.05",
    "TEMP_CHECK_HTTP_TIMEOUT": "500",
}


async def _start_temp(tmp_path, temps=None, over=None):
    """Proxy + LLM backend + a mock temperature endpoint. temps=None keeps
    the endpoint unreachable (dead port)."""
    calls = {"n": 0}

    async def chat(request):
        calls["n"] += 1
        return web.json_response(CHAT)

    backend_app = web.Application()
    backend_app.router.add_post("/v1/chat/completions", chat)
    backend = TestServer(backend_app)
    await backend.start_server()

    state = {"temps": temps}

    async def temperature(request):
        if state["temps"] is None:
            return web.Response(status=500)
        return web.json_response(state["temps"])

    temp_app = web.Application()
    temp_app.router.add_get("/temperatures", temperature)
    temp = TestServer(temp_app)
    await temp.start_server()

    env = dict(TEMP_ENV)
    env["TEMP_CHECK_ENDPOINT"] = f"http://127.0.0.1:{temp.port}/temperatures"
    env["TEMP_CHECK_STALL_LOG"] = str(tmp_path / "stall.log")
    env.update(over or {})
    cfg = _cfg(tmp_path, f"127.0.0.1:{backend.port}", **env)
    proxy = TestClient(TestServer(create_app(cfg)))
    await proxy.start_server()
    return backend, proxy, temp, state, calls


def test_temp_fail_open_forwards_when_endpoint_down(tmp_path):
    """Endpoint down + TEMP_CHECK_FAIL_OPEN=1: the request is forwarded
    instead of being stalled."""
    async def main():
        backend, proxy, temp, state, calls = await _start_temp(
            tmp_path, temps=None, over={"TEMP_CHECK_FAIL_OPEN": "1"})
        try:
            async with await asyncio.wait_for(
                    proxy.post("/v1/chat/completions",
                               json={"model": "m", "stream": False}), 5) as r:
                assert r.status == 200
            assert calls["n"] == 1
            st = await _state(proxy)
            tg = st["temp_guard"]
            assert tg["config_enabled"] is True and tg["fail_open"] is True
            assert tg["last_error"] is not None
        finally:
            await proxy.close(); await backend.close(); await temp.close()
    asyncio.run(main())


def test_temp_fail_closed_stalls_until_dashboard_disable(tmp_path):
    """Endpoint down, default fail-closed: the request stalls (no reply)
    and is released when the guard is switched off via the dashboard."""
    async def main():
        backend, proxy, temp, state, calls = await _start_temp(
            tmp_path, temps=None)
        try:
            task = asyncio.create_task(
                proxy.post("/v1/chat/completions",
                           json={"model": "m", "stream": False}))
            try:
                await asyncio.wait_for(asyncio.shield(task), 1.5)
                assert False, "fail-closed must stall the request"
            except asyncio.TimeoutError:
                pass
            assert calls["n"] == 0
            st = await _state(proxy)
            assert st["temp_guard"]["enabled"] is True
            assert st["temp_guard"]["last_error"] is not None

            # Toggle the guard off at runtime.
            async with proxy.post("/api/dashboard/tempguard",
                                  json={"enabled": False}) as r:
                assert r.status == 200
                assert await r.json() == {"ok": True, "enabled": False}
            async with await asyncio.wait_for(task, 10) as r:
                assert r.status == 200
                assert (await r.json())["object"] == "chat.completion"
            assert calls["n"] == 1

            st = await _state(proxy)
            assert st["temp_guard"]["enabled"] is False
            assert st["temp_guard"]["user_enabled"] is False
            assert st["temp_guard"]["config_enabled"] is True
        finally:
            await proxy.close(); await backend.close(); await temp.close()
    asyncio.run(main())


def test_temp_guard_stalls_hot_and_releases_when_cool(tmp_path):
    """Guard on + endpoint up: hot GPU stalls, cooling down releases, and
    re-enabling via the dashboard is reported in state."""
    async def main():
        backend, proxy, temp, state, calls = await _start_temp(
            tmp_path, temps={"gpu_0": 80})
        try:
            task = asyncio.create_task(
                proxy.post("/v1/chat/completions",
                           json={"model": "m", "stream": False}))
            try:
                await asyncio.wait_for(asyncio.shield(task), 1.0)
                assert False, "hot GPU must stall the request"
            except asyncio.TimeoutError:
                pass
            assert calls["n"] == 0
            st = await _state(proxy)
            assert st["temp_guard"]["max_temp"] == 80

            state["temps"] = {"gpu_0": 40}  # GPU cooled
            async with await asyncio.wait_for(task, 10) as r:
                assert r.status == 200
            assert calls["n"] == 1

            # Re-enable endpoint round-trip (already on -> stays on).
            async with proxy.post("/api/dashboard/tempguard",
                                  json={"enabled": True}) as r:
                assert await r.json() == {"ok": True, "enabled": True}
            # Malformed body -> 400.
            async with proxy.post("/api/dashboard/tempguard",
                                  json={"nope": 1}) as r:
                assert r.status == 400
        finally:
            await proxy.close(); await backend.close(); await temp.close()
    asyncio.run(main())


def test_temp_state_when_guard_disabled_globally(tmp_path):
    """TEMP_CHECK_ENABLED=0: state reports the guard inactive regardless
    of the runtime switch."""
    async def main():
        backend, proxy, _ = await _start(tmp_path)
        try:
            st = await _state(proxy)
            tg = st["temp_guard"]
            assert tg["enabled"] is False and tg["config_enabled"] is False
            assert tg["user_enabled"] is True
            async with proxy.post("/api/dashboard/tempguard",
                                  json={"enabled": False}) as r:
                assert r.status == 200
            st = await _state(proxy)
            assert st["temp_guard"]["user_enabled"] is False
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


# ------------------------------------------------- runtime threshold editing

async def _thresholds(proxy, body, expect=200):
    async with proxy.post("/api/dashboard/tempguard/thresholds",
                          json=body) as r:
        assert r.status == expect, f"{body} -> HTTP {r.status}"
        assert r.headers.get("Access-Control-Allow-Origin") == "*"
        return await r.json()


def test_temp_thresholds_edit_and_reset(tmp_path):
    """The live hysteresis pair is editable at runtime, reported in state
    (only while the guard is armed by config), and resettable to .env."""
    async def main():
        backend, proxy, temp, state, calls = await _start_temp(
            tmp_path, temps={"gpu_0": 50})
        try:
            tg = (await _state(proxy))["temp_guard"]
            assert (tg["stall_above"], tg["resume_below"]) == (74.0, 60.0)
            assert (tg["default_stall_above"], tg["default_resume_below"]) \
                == (74.0, 60.0)

            out = await _thresholds(proxy, {"stall_above": 50,
                                            "resume_below": 30})
            assert out == {"ok": True, "stall_above": 50.0,
                           "resume_below": 30.0}
            tg = (await _state(proxy))["temp_guard"]
            assert (tg["stall_above"], tg["resume_below"]) == (50.0, 30.0)
            # the .env defaults survive an edit for the reset button
            assert (tg["default_stall_above"], tg["default_resume_below"]) \
                == (74.0, 60.0)

            # Partial edit: the omitted key keeps its live value.
            out = await _thresholds(proxy, {"resume_below": 20})
            assert (out["stall_above"], out["resume_below"]) == (50.0, 20.0)

            out = await _thresholds(proxy, {"reset": True})
            assert (out["stall_above"], out["resume_below"]) == (74.0, 60.0)
            tg = (await _state(proxy))["temp_guard"]
            assert (tg["stall_above"], tg["resume_below"]) == (74.0, 60.0)
        finally:
            await proxy.close(); await backend.close(); await temp.close()
    asyncio.run(main())


def test_temp_thresholds_change_live_behaviour(tmp_path):
    """A lowered threshold stalls a temperature the .env pair let
    through, and the stalled request is released by further cooling —
    the edit is not merely cosmetic."""
    async def main():
        backend, proxy, temp, state, calls = await _start_temp(
            tmp_path, temps={"gpu_0": 60})
        try:
            # 60°C is below the .env stall_above=74: forwarded at once.
            async with await asyncio.wait_for(
                    proxy.post("/v1/chat/completions",
                               json={"model": "m", "stream": False}), 5) as r:
                assert r.status == 200
            assert calls["n"] == 1

            await _thresholds(proxy, {"stall_above": 50, "resume_below": 30})

            # The same 60°C now stalls: no reply, no further upstream call.
            task = asyncio.create_task(
                proxy.post("/v1/chat/completions",
                           json={"model": "m", "stream": False}))
            try:
                await asyncio.wait_for(asyncio.shield(task), 1.5)
                assert False, "60°C must stall once stall_above is 50"
            except asyncio.TimeoutError:
                pass
            assert calls["n"] == 1, "stalled request must not reach the backend"

            # 40°C is above the new resume_below=30: still held.
            state["temps"] = {"gpu_0": 40}
            await asyncio.sleep(0.6)
            assert calls["n"] == 1

            # 20°C releases it.
            state["temps"] = {"gpu_0": 20}
            async with await asyncio.wait_for(task, 10) as r:
                assert r.status == 200
            assert calls["n"] == 2
        finally:
            await proxy.close(); await backend.close(); await temp.close()
    asyncio.run(main())


def test_temp_thresholds_validation_keeps_live_values(tmp_path):
    """A rejected pair comes back as 400 with a reason and leaves the
    live thresholds untouched."""
    async def main():
        backend, proxy, temp, state, calls = await _start_temp(
            tmp_path, temps={"gpu_0": 50})
        try:
            bad = [
                ({"stall_above": 30, "resume_below": 60}, "lower"),   # inverted
                ({"stall_above": 60, "resume_below": 60}, "lower"),   # equal
                ({"stall_above": 9999}, "between"),                   # out of range
                ({"resume_below": -51}, "between"),                   # out of range
                ({"stall_above": "hot"}, "number"),                   # not a number
                ({"stall_above": 70, "resume_below": 60, "x": 1}, "unexpected"),
                ({}, "stall_above"),                                  # nothing to do
            ]
            for body, phrase in bad:
                out = await _thresholds(proxy, body, expect=400)
                assert out["ok"] is False
                assert phrase in out["error"], f"{body} -> {out['error']}"
            tg = (await _state(proxy))["temp_guard"]
            assert (tg["stall_above"], tg["resume_below"]) == (74.0, 60.0)

            # Boundary values are accepted (they are sane, if silly).
            await _thresholds(proxy, {"stall_above": 250, "resume_below": -50})
        finally:
            await proxy.close(); await backend.close(); await temp.close()
    asyncio.run(main())


def test_temp_thresholds_require_config_and_reach_state(tmp_path):
    """With the guard disarmed by config there is nothing to tune (400),
    and state omits the threshold keys entirely."""
    async def main():
        backend, proxy, _ = await _start(tmp_path)
        try:
            tg = (await _state(proxy))["temp_guard"]
            for key in ("stall_above", "resume_below",
                        "default_stall_above", "default_resume_below"):
                assert key not in tg
            out = await _thresholds(proxy, {"stall_above": 70}, expect=400)
            assert out["ok"] is False
            assert "disabled by config" in out["error"]
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_temp_checked_at_is_wall_clock(tmp_path):
    """state.checked_at is wall-clock, not the monotonic clock the cache
    uses internally: a monotonic value would make the dashboard's
    "checked N ago" age a nonsense countdown from container start."""
    async def main():
        backend, proxy, temp, state, calls = await _start_temp(
            tmp_path, temps={"gpu_0": 48})
        try:
            # No snapshot fetched yet.
            assert (await _state(proxy))["temp_guard"]["checked_at"] is None

            # Any forwarded request fetches a snapshot.
            async with await asyncio.wait_for(
                    proxy.post("/v1/chat/completions",
                               json={"model": "m", "stream": False}), 5) as r:
                assert r.status == 200
            before = time.time()
            checked = (await _state(proxy))["temp_guard"]["checked_at"]
            assert isinstance(checked, (int, float))
            assert before - 10 <= checked <= time.time() + 10
        finally:
            await proxy.close(); await backend.close(); await temp.close()
    asyncio.run(main())


def test_validate_thresholds():
    """Unit-level rules behind the endpoint's 400s."""
    from llm_proxy.tempguard import validate_thresholds

    assert validate_thresholds(74, 60) is None
    assert validate_thresholds(250, -50) is None       # both boundaries
    assert "lower" in validate_thresholds(60, 60)      # equal is not below
    assert "lower" in validate_thresholds(30, 60)      # inverted
    assert "between" in validate_thresholds(251, 60)
    assert "between" in validate_thresholds(74, -51)
    assert "finite" in validate_thresholds(float("nan"), 60)
    assert "finite" in validate_thresholds(74, float("inf"))
    assert "finite" in validate_thresholds("74", 60)   # str
    assert "finite" in validate_thresholds(74, True)   # bool is not a number


# ------------------------------------------------------------------ drop all

def _slow_backend(delay=2.0):
    """Mock backend handler: counts its invocations and stalls, so the
    proxy is provably mid-upstream when the drop lands. The stall is
    cancellable, so a cancelled upstream fetch also stops the backend."""
    seen = {"n": 0}

    async def handler(request):
        seen["n"] += 1
        await asyncio.sleep(delay)
        return web.json_response(CHAT)

    return handler, seen


async def _wait_empty(proxy, timeout=5.0):
    """Poll /state until the registry is empty (drop_all already waits
    for the handlers to unwind, but the client-side teardown is not
    synchronous with the response)."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        st = await _state(proxy)
        if not st["in_flight"]:
            return
        if loop.time() >= deadline:
            assert False, f"registry must be clean, still: {st['in_flight']}"
        await asyncio.sleep(0.05)


def test_inflight_add_ids_are_unique_within_a_millisecond():
    """The caller's rec_id is a ms timestamp; two requests in the same ms
    must NOT overwrite each other's record or kill entry (point 8)."""
    from llm_proxy.inflight import InFlight

    reg = InFlight()
    a = reg.add("1700000000000", model="m", path="/v1/completions",
                stream=False)
    b = reg.add("1700000000000", model="m", path="/v1/completions",
                stream=False)
    assert a["id"] != b["id"], "same-ms ids must not collide"
    assert len(reg.list_inflight()) == 2
    # Kill entries keyed by the record's own id stay separate too.
    reg.register_kill(a["id"], {}, None, None)
    reg.register_kill(b["id"], {}, None, None)
    assert len(reg._kill) == 2
    reg.remove(a["id"])
    assert len(reg._kill) == 1
    assert [r["id"] for r in reg.list_inflight()] == [b["id"]]


def test_drop_nonstream_mid_upstream(tmp_path):
    """POST /api/dashboard/drop while a request is mid-upstream: the client
    gets a connection error (RST, no response), the registry empties, the
    proxy lives and answers the next request. The proxy runs with
    handler_cancellation=True like production (web.run_app)."""
    async def main():
        slow, seen = _slow_backend()
        backend, proxy, calls = await _start(tmp_path, handler=slow, hc=True)
        try:
            task = asyncio.create_task(
                proxy.post("/v1/chat/completions",
                           json={"model": "m", "stream": False}))
            await asyncio.sleep(0.3)      # inside the upstream call
            assert calls["n"] == 1
            st = await _state(proxy)
            assert len(st["in_flight"]) == 1
            assert st["in_flight"][0]["state"] == "upstream"

            out = await _drop(proxy)
            assert out == {"ok": True, "dropped": 1}

            # The client sees the severed connection, not a response.
            await _gone(asyncio.wait_for(task, 10))

            # drop_all already waited for the handlers, so the registry is
            # empty by the time the drop response was sent; the poll is
            # belt-and-braces, not a sleep.
            await _wait_empty(proxy)
            st = await _state(proxy)
            assert st["in_flight"] == []
            async with proxy.get("/health") as h:
                assert h.status == 200
                assert await h.text() == "ok"

            # The proxy answers the NEXT request, and the dropped one never
            # resurfaces (one call per client request).
            async with await asyncio.wait_for(
                    proxy.post("/v1/chat/completions",
                               json={"model": "m", "stream": False}), 5) as r:
                assert r.status == 200
                assert (await r.json())["object"] == "chat.completion"
            assert calls["n"] == 2
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_drop_stream_after_handshake_commit(tmp_path):
    """stream:true whose deferred SSE handshake already committed (heartbeat
    received): the drop kills the connection mid-body, the stream never
    completes with [DONE], and the registry empties."""
    async def main():
        slow, seen = _slow_backend()
        backend, proxy, calls = await _start(tmp_path, handler=slow, hc=True)
        try:
            r = await proxy.post("/v1/chat/completions",
                                 json={"model": "m", "stream": True})
            assert r.status == 200
            assert r.headers["Content-Type"] == "text/event-stream"
            first = (await asyncio.wait_for(r.content.read(4096), 5)).decode()
            assert _is_heartbeat(first.splitlines()[0])
            assert calls["n"] == 1

            out = await _drop(proxy)
            assert out == {"ok": True, "dropped": 1}

            whole = first
            try:
                whole += await _collect(r)
                assert False, "dropped stream must not complete"
            except (aiohttp.ClientError, OSError):
                pass
            assert not whole.rstrip().endswith("data: [DONE]")
            assert _content_of(whole) == ""

            await _wait_empty(proxy)
            async with proxy.get("/health") as h:
                assert h.status == 200
            # The dropped upstream fetch died with the handler: the backend
            # call count stopped growing (no task.cancelled() race).
            assert calls["n"] == 1, "the cancelled upstream fetch must die too"
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_drop_request_held_by_pause_gate(tmp_path):
    """A request held by the pause gate (state "paused") is dropped too:
    the client sees the connection error, the registry empties, and the
    pause state itself is untouched. A pre-upstream drop writes NO stats
    line (write_stats returns early without _sent_at) — deliberately not
    asserted here."""
    async def main():
        backend, proxy, calls = await _start(tmp_path, hc=True)
        try:
            await _pause(proxy)
            task = asyncio.create_task(
                proxy.post("/v1/chat/completions",
                           json={"model": "m", "stream": False}))
            try:
                await asyncio.wait_for(asyncio.shield(task), 1.5)
                assert False, "paused request must not respond"
            except asyncio.TimeoutError:
                pass
            st = await _state(proxy)
            assert st["in_flight"][0]["state"] == "paused"
            assert calls["n"] == 0

            out = await _drop(proxy)
            assert out == {"ok": True, "dropped": 1}

            await _gone(asyncio.wait_for(task, 10))

            await _wait_empty(proxy)
            st = await _state(proxy)
            assert st["paused"] is True, "the drop does not resume the proxy"
            assert st["paused_at"] is not None
            async with proxy.get("/health") as h:
                assert h.status == 200
            # The gate is still armed: resume, and a fresh request works.
            await _resume(proxy)
            async with await asyncio.wait_for(
                    proxy.post("/v1/chat/completions",
                               json={"model": "m", "stream": False}), 10) as r:
                assert r.status == 200
                assert (await r.json())["object"] == "chat.completion"
            assert calls["n"] == 1
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_drop_with_no_inflight(tmp_path):
    """Nothing in flight: the endpoint is a no-op."""
    async def main():
        backend, proxy, _ = await _start(tmp_path, hc=True)
        try:
            assert await _drop(proxy) == {"ok": True, "dropped": 0}
            # The action is still recorded on the event feed.
            assert [e["action"] for e in (await _state(proxy))["events"]] \
                == ["drop"]
            async with proxy.get("/health") as h:
                assert h.status == 200
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_drop_reports_count_and_records_event(tmp_path):
    """The dropped count matches the in-flight list, the event feed carries
    the drop, and the kill map is not left behind (second drop = 0)."""
    async def main():
        slow, seen = _slow_backend()
        backend, proxy, calls = await _start(tmp_path, handler=slow, hc=True)
        try:
            t1 = asyncio.create_task(
                proxy.post("/v1/chat/completions",
                           json={"model": "m", "stream": False}))
            await asyncio.sleep(0.1)
            t2 = asyncio.create_task(
                proxy.post("/v1/chat/completions",
                           json={"model": "m", "stream": True}))
            await asyncio.sleep(0.3)
            assert calls["n"] == 2
            assert len((await _state(proxy))["in_flight"]) == 2

            out = await _drop(proxy)
            assert out["ok"] is True
            assert out["dropped"] == 2
            st = await _state(proxy)
            assert [e["action"] for e in st["events"]] == ["drop"]
            assert isinstance(st["events"][0]["ts"], (int, float))

            for t in (t1, t2):
                try:
                    await t
                except (aiohttp.ClientError, OSError):
                    pass
            await _wait_empty(proxy)
            # A second drop has nothing left to drop: the kill map went too.
            assert await _drop(proxy) == {"ok": True, "dropped": 0}
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_drop_preflight_does_not_drop(tmp_path):
    """The CORS preflight must not trigger the drop (the _api guard)."""
    async def main():
        slow, seen = _slow_backend()
        backend, proxy, calls = await _start(tmp_path, handler=slow, hc=True)
        try:
            task = asyncio.create_task(
                proxy.post("/v1/chat/completions",
                           json={"model": "m", "stream": False}))
            await asyncio.sleep(0.3)
            assert calls["n"] == 1
            async with proxy.options("/api/dashboard/drop") as r:
                assert r.status == 204
            async with await asyncio.wait_for(task, 10) as r:
                assert r.status == 200
                assert (await r.json())["object"] == "chat.completion"
            assert calls["n"] == 1
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())
