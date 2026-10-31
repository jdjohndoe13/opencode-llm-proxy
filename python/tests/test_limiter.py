"""Tests for the concurrent-request limiter (llm_proxy.limiter).

Two layers: unit tests for the semaphore itself (FIFO, abort, dynamic
limit changes), and E2E tests through the real proxy for the parts that
only exist once it is wired into the request pipeline — the /state
reporting, the dashboard endpoint, and "one departure admits exactly one
waiter".

The E2E tests use a backend that blocks until the test releases it, so the
number of requests that reached the LLM server is directly observable.
"""
import asyncio
import json

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from llm_proxy.config import Config, ConfigError, load
from llm_proxy.limiter import ConcurrencyLimiter, validate_limit
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


# ------------------------------------------------------------ unit: config

def test_concurrency_limit_defaults_to_unlimited():
    cfg = load({"LLM_BACKEND": "b:1", "KEEPALIVE_DELAY": "1",
                "KEEPALIVE_INTERVAL": "1", "UPSTREAM_TIMEOUT": "5",
                "UPSTREAM_RETRIES": "1"})
    assert cfg.concurrency_limit == 0


def test_concurrency_limit_parsed_and_validated():
    def parse(v):
        return load({"LLM_BACKEND": "b:1", "KEEPALIVE_DELAY": "1",
                     "KEEPALIVE_INTERVAL": "1", "UPSTREAM_TIMEOUT": "5",
                     "UPSTREAM_RETRIES": "1", "CONCURRENCY_LIMIT": v})
    assert parse("4").concurrency_limit == 4
    assert parse("0").concurrency_limit == 0
    # empty means "not set", like TOP_P: compose passes optional vars as ""
    assert parse("").concurrency_limit == 0
    for bad in ("-1", "1.5", "abc"):
        with pytest.raises(ConfigError):
            parse(bad)


def test_validate_limit():
    assert validate_limit(0) is None                 # unlimited
    assert validate_limit(1) is None
    assert validate_limit(10000) is None             # boundary
    assert "whole number" in validate_limit(1.5)
    assert "whole number" in validate_limit("4")     # str is not a number
    assert "whole number" in validate_limit(True)    # bool is not a number
    assert "whole number" in validate_limit(float("nan"))
    assert "between" in validate_limit(-1)
    assert "between" in validate_limit(10001)


def _limiter(limit=0):
    cfg = Config()
    cfg.concurrency_limit = limit
    return ConcurrencyLimiter(cfg)


# ------------------------------------------------------- unit: the gate

def test_unlimited_never_queues():
    async def main():
        lim = _limiter(0)
        ctxs = [{} for _ in range(50)]
        for ctx in ctxs:
            assert await lim.acquire(ctx) is True
        st = lim.status()
        assert st["queued"] == 0 and st["active"] == 50
    asyncio.run(main())


def test_limit_caps_active_and_queues_the_rest():
    async def main():
        lim = _limiter(2)
        a, b = {}, {}
        assert await lim.acquire(a) is True
        assert await lim.acquire(b) is True
        assert lim.status() == {"limit": 2, "active": 2, "queued": 0,
                                "default_limit": 2}

        c = {}
        task = asyncio.create_task(lim.acquire(c))
        await asyncio.sleep(0.35)          # one tick inside the wait loop
        assert not task.done()
        assert lim.status()["queued"] == 1

        # one departure admits exactly one waiter, oldest first
        lim.release()
        assert await asyncio.wait_for(task, 2) is True
        assert lim.status() == {"limit": 2, "active": 2, "queued": 0,
                                "default_limit": 2}
        assert c["_slot_held"] is True
        assert c["_queued_seconds"] >= 0
        lim.release()
        lim.release()
        assert lim.status()["active"] == 0
    asyncio.run(main())


def test_queue_is_fifo():
    async def main():
        lim = _limiter(1)
        holder = {}
        await lim.acquire(holder)
        order = []
        tasks = []
        for i in range(3):
            ctx = {}
            async def wait(i=i, ctx=ctx):
                if await lim.acquire(ctx):
                    order.append(i)
            tasks.append(asyncio.create_task(wait()))
        await asyncio.sleep(0.35)
        assert lim.status()["queued"] == 3
        for _ in range(3):
            lim.release()
            await asyncio.sleep(0.35)
        await asyncio.gather(*tasks)
        assert order == [0, 1, 2]
        assert lim.status()["active"] == 1           # last one still holds
        lim.release()
    asyncio.run(main())


def test_abort_while_queued_releases_no_slot():
    """A client that goes away while queued must not consume a slot, and
    must not leave a dead reservation in the queue."""
    async def main():
        lim = _limiter(1)
        holder = {}
        await lim.acquire(holder)
        ctx = {"_aborted": True}
        assert await lim.acquire(ctx) is False
        assert "_slot_held" not in ctx
        assert lim.status() == {"limit": 1, "active": 1, "queued": 0,
                                "default_limit": 1}
        # the waiter that arrived after the abort still gets the slot
        nxt = {}
        task = asyncio.create_task(lim.acquire(nxt))
        await asyncio.sleep(0.35)
        assert not task.done()
        lim.release()
        assert await asyncio.wait_for(task, 2) is True
    asyncio.run(main())


def test_cancellation_while_queued_returns_the_granted_slot():
    """handler_cancellation can hit the instant a grant lands. The slot
    reserved for a cancelled waiter must go back to the pool."""
    async def main():
        lim = _limiter(1)
        holder = {}
        await lim.acquire(holder)
        victim = asyncio.create_task(lim.acquire({}))
        await asyncio.sleep(0.35)
        assert lim.status()["queued"] == 1
        victim.cancel()
        await asyncio.sleep(0.35)
        assert lim.status() == {"limit": 1, "active": 1, "queued": 0,
                                "default_limit": 1}
        lim.release()
        assert lim.status()["active"] == 0
        # and the pool is still usable
        assert await lim.acquire({}) is True
    asyncio.run(main())


def test_raising_the_limit_admits_queued_requests():
    async def main():
        lim = _limiter(1)
        holder = {}
        await lim.acquire(holder)
        ctx = {}
        task = asyncio.create_task(lim.acquire(ctx))
        await asyncio.sleep(0.35)
        assert not task.done()

        ok, err = lim.set_limit(2)
        assert ok and err is None
        assert await asyncio.wait_for(task, 2) is True   # admitted, no release
        assert lim.status()["active"] == 2
    asyncio.run(main())


def test_lowering_the_limit_does_not_disturb_active_requests():
    async def main():
        lim = _limiter(3)
        holders = [{}, {}, {}]
        for ctx in holders:
            await lim.acquire(ctx)
        # a request already holding a slot keeps it; only new ones queue
        ok, _ = lim.set_limit(1)
        assert ok
        assert lim.status()["active"] == 3
        ctx = {}
        task = asyncio.create_task(lim.acquire(ctx))
        await asyncio.sleep(0.35)
        assert not task.done()
        for _ in range(3):
            lim.release()
        await asyncio.wait_for(task, 2)
        assert lim.status()["active"] == 1
    asyncio.run(main())


def test_reset_restores_the_env_default():
    lim = _limiter(4)
    ok, _ = lim.set_limit(9)
    assert ok and lim.limit == 9
    lim.reset_limit()
    assert lim.limit == 4
    assert lim.default_limit == 4           # the .env value survives edits


def test_set_limit_rejects_garbage_and_keeps_the_live_value():
    lim = _limiter(4)
    for bad in (1.5, -1, 10001, "4", True, float("nan")):
        ok, err = lim.set_limit(bad)
        assert ok is False and err
        assert lim.limit == 4
    assert lim.set_limit(0) == (True, None)  # unlimited is a valid choice


# ------------------------------------------------------------- E2E helpers

def _cfg(tmp_path, backend, **over):
    env = {
        "LLM_BACKEND": backend, "KEEPALIVE_DELAY": "1",
        "KEEPALIVE_INTERVAL": "1", "UPSTREAM_TIMEOUT": "5",
        "UPSTREAM_RETRIES": "1", "LOG_DIR": str(tmp_path),
    }
    env.update(over)
    return load(env)


async def _start_gated(tmp_path, limit):
    """Proxy + backend that holds every request until the test releases it.

    Returns (backend, proxy, calls, release). `calls["n"]` is how many
    requests reached the LLM server; `await release()` lets one finish."""
    calls = {"n": 0}
    gates = []

    async def chat(request):
        calls["n"] += 1
        g = asyncio.Event()
        gates.append(g)
        await g.wait()
        return web.json_response(CHAT)

    async def release():
        assert gates, "no in-flight request to release"
        gates.pop(0).set()

    backend_app = web.Application()
    backend_app.router.add_post("/v1/chat/completions", chat)
    backend = TestServer(backend_app)
    await backend.start_server()
    cfg = _cfg(tmp_path, f"127.0.0.1:{backend.port}",
               **{"CONCURRENCY_LIMIT": str(limit)})
    proxy = TestClient(TestServer(create_app(cfg)))
    await proxy.start_server()
    return backend, proxy, calls, release


async def _state(proxy):
    async with proxy.get("/api/dashboard/state") as r:
        assert r.status == 200
        return await r.json()


def _send(proxy):
    return proxy.post("/v1/chat/completions",
                      json={"model": "m", "stream": False})


async def _set_limit(proxy, body, expect=200):
    async with proxy.post("/api/dashboard/concurrency", json=body) as r:
        assert r.status == expect, f"{body} -> HTTP {r.status}"
        assert r.headers.get("Access-Control-Allow-Origin") == "*"
        return await r.json()


async def _wait_calls(calls, n, timeout=3.0):
    """Wait until exactly n requests have reached the backend."""
    async def watch():
        while calls["n"] < n:
            await asyncio.sleep(0.05)
    await asyncio.wait_for(watch(), timeout)


# ------------------------------------------------------------- E2E: limiting

def test_limit_blocks_the_excess_and_a_departure_admits_one(tmp_path):
    """The user's scenario: limit 4, four requests pass, the fifth is held,
    and one completion releases exactly one held request."""
    async def main():
        backend, proxy, calls, release = await _start_gated(tmp_path, 4)
        tasks = []
        try:
            for _ in range(4):
                tasks.append(asyncio.create_task(_send(proxy)))
            await _wait_calls(calls, 4)

            fifth = asyncio.create_task(_send(proxy))
            await asyncio.sleep(0.6)
            assert calls["n"] == 4, "the fifth request must not reach the LLM"
            assert not fifth.done()

            st = (await _state(proxy))["concurrency"]
            assert st == {"limit": 4, "active": 4, "queued": 1,
                          "default_limit": 4}

            # one completion -> exactly one admit
            await release()
            await _wait_calls(calls, 5)
            await asyncio.sleep(0.6)
            assert calls["n"] == 5, "a departure must admit exactly one"

            st = (await _state(proxy))["concurrency"]
            assert st["active"] == 4 and st["queued"] == 0

            for _ in range(4):
                await release()
            for t in tasks + [fifth]:
                async with await asyncio.wait_for(t, 5) as r:
                    assert r.status == 200
        finally:
            for t in tasks:
                t.cancel()
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_default_is_unlimited(tmp_path):
    """No CONCURRENCY_LIMIT: everything passes straight through."""
    async def main():
        backend, proxy, calls, release = await _start_gated(tmp_path, 0)
        tasks = []
        try:
            for _ in range(6):
                tasks.append(asyncio.create_task(_send(proxy)))
            await _wait_calls(calls, 6)
            st = (await _state(proxy))["concurrency"]
            assert st["limit"] == 0 and st["queued"] == 0
            assert st["active"] == 6
            for _ in range(6):
                await release()
            for t in tasks:
                async with await asyncio.wait_for(t, 5) as r:
                    assert r.status == 200
        finally:
            for t in tasks:
                t.cancel()
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_lowering_the_limit_grandfathers_active_requests(tmp_path):
    """Lowering the cap never disturbs requests that already hold a slot:
    they keep running, and the queue only drains once the pool has actually
    fallen under the new limit."""
    async def main():
        backend, proxy, calls, release = await _start_gated(tmp_path, 0)
        tasks = []
        try:
            for _ in range(2):
                tasks.append(asyncio.create_task(_send(proxy)))
            await _wait_calls(calls, 2)

            out = await _set_limit(proxy, {"limit": 1})
            assert out["ok"] is True and out["limit"] == 1
            # both in-flight requests keep their slots
            assert (await _state(proxy))["concurrency"]["active"] == 2

            third = asyncio.create_task(_send(proxy))
            await asyncio.sleep(0.6)
            assert calls["n"] == 2, "a new request must not exceed the cap"
            assert not third.done()

            # one departure leaves 2-1=1 active, which is not below the
            # limit of 1: still no admission
            await release()
            await asyncio.sleep(0.6)
            assert calls["n"] == 2
            assert (await _state(proxy))["concurrency"] == {
                "limit": 1, "active": 1, "queued": 1, "default_limit": 0}

            # the pool is now empty, so the waiter is admitted
            await release()
            await _wait_calls(calls, 3)
            assert (await _state(proxy))["concurrency"]["queued"] == 0
            await release()
            async with await asyncio.wait_for(third, 5) as r:
                assert r.status == 200
        finally:
            for t in tasks:
                t.cancel()
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_queued_requests_are_reported_in_flight(tmp_path):
    """The held request is visible in the in-flight table as `queued`."""
    async def main():
        backend, proxy, calls, release = await _start_gated(tmp_path, 1)
        task = None
        try:
            holder = asyncio.create_task(_send(proxy))
            await _wait_calls(calls, 1)
            task = asyncio.create_task(_send(proxy))
            await asyncio.sleep(0.6)

            states = sorted(r["state"]
                            for r in (await _state(proxy))["in_flight"])
            assert states == ["queued", "upstream"]
            await release()
            await _wait_calls(calls, 2)
            await release()
            async with await asyncio.wait_for(task, 5) as r:
                assert r.status == 200
            async with await asyncio.wait_for(holder, 5) as r:
                assert r.status == 200
        finally:
            for t in (task, holder):
                if t is not None:
                    t.cancel()
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_concurrency_endpoint_roundtrip_and_reset(tmp_path):
    """Edit, partial-state reporting, reset, and the .env default."""
    async def main():
        backend, proxy, calls, release = await _start_gated(tmp_path, 4)
        try:
            out = await _set_limit(proxy, {"limit": 7})
            assert out == {"ok": True, "limit": 7, "default_limit": 4}
            st = (await _state(proxy))["concurrency"]
            assert (st["limit"], st["default_limit"]) == (7, 4)

            out = await _set_limit(proxy, {"reset": True})
            assert out == {"ok": True, "limit": 4, "default_limit": 4}
            assert (await _state(proxy))["concurrency"]["limit"] == 4

            # 0 is a valid limit: back to unlimited
            assert (await _set_limit(proxy, {"limit": 0}))["limit"] == 0
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_concurrency_endpoint_validation(tmp_path):
    """A rejected edit comes back as 400 with a reason and changes nothing."""
    async def main():
        backend, proxy, calls, release = await _start_gated(tmp_path, 4)
        try:
            bad = [
                ({"limit": 1.5}, "whole number"),
                ({"limit": -1}, "between"),
                ({"limit": 10001}, "between"),
                ({"limit": "four"}, "whole number"),
                ({"limit": 4, "x": 1}, "unexpected"),
                ({}, "limit"),
            ]
            for body, phrase in bad:
                out = await _set_limit(proxy, body, expect=400)
                assert out["ok"] is False
                assert phrase in out["error"], f"{body} -> {out['error']}"
            assert (await _state(proxy))["concurrency"]["limit"] == 4
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())
