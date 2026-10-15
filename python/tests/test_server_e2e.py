"""E2E tests: the proxy against a mock backend on an ephemeral port.

The real proxy (9999) is never touched: every test binds port 0.
"""
import asyncio
import json

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from llm_proxy.config import load
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
        "LLM_BACKEND": backend, "KEEPALIVE_DELAY": "0.3",
        "KEEPALIVE_INTERVAL": "0.3", "UPSTREAM_TIMEOUT": "5",
        "UPSTREAM_RETRIES": "1", "LOG_DIR": str(tmp_path),
    }
    env.update(over)
    return load(env)


def _backend(handler, captured):
    app = web.Application()

    async def chat(request):
        captured["body"] = await request.json()
        return await handler(request)

    async def models(request):
        return web.json_response({"data": ["m"], "object": "list"})

    app.router.add_post("/v1/chat/completions", chat)
    app.router.add_get("/v1/models", models)
    return app


async def _with_servers(tmp_path, handler, over=None):
    captured = {}
    backend = TestServer(_backend(handler, captured))
    await backend.start_server()
    cfg = _cfg(tmp_path, f"127.0.0.1:{backend.port}", **(over or {}))
    proxy = TestClient(TestServer(create_app(cfg)))
    await proxy.start_server()
    return backend, proxy, captured


async def _ok_chat(request):
    return web.json_response(CHAT)


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


def test_stream_true_rewrites_and_streams(tmp_path):
    async def main():
        backend, proxy, captured = await _with_servers(tmp_path, _ok_chat)
        try:
            async with proxy.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [], "stream": True,
                      "stream_options": {"include_usage": True}}) as r:
                assert r.status == 200
                assert r.headers["Content-Type"] == "text/event-stream"
                whole = await _collect(r)
            assert captured["body"]["stream"] is False
            assert "stream_options" not in captured["body"]
            assert _content_of(whole) == "hello world"
            assert whole.rstrip().endswith("data: [DONE]")
            # dumps + stats written
            assert any(p.name.endswith(".req.json") for p in tmp_path.iterdir())
            assert any(p.name.endswith(".resp.json") for p in tmp_path.iterdir())
            stats = (tmp_path / "stats.jsonl").read_text(encoding="utf-8")
            entry = json.loads(stats.strip())
            assert entry["model"] == "m" and entry["status"] == 200
            assert entry["prompt_tokens"] == 3
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_stream_false_plain_relay(tmp_path):
    async def main():
        backend, proxy, captured = await _with_servers(tmp_path, _ok_chat)
        try:
            async with proxy.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [], "stream": False}) as r:
                assert r.status == 200
                assert r.headers["Content-Type"].startswith("application/json")
                body = await r.json()
            assert body["object"] == "chat.completion"
            assert captured["body"]["stream"] is False
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_top_p_forced(tmp_path):
    async def main():
        backend, proxy, captured = await _with_servers(
            tmp_path, _ok_chat, {"TOP_P": "0.5"})
        try:
            await proxy.post(
                "/v1/chat/completions",
                json={"model": "m", "top_p": 0.9, "stream": False})
            assert captured["body"]["top_p"] == 0.5
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_top_p_default_untouched(tmp_path):
    async def main():
        backend, proxy, captured = await _with_servers(tmp_path, _ok_chat)
        try:
            await proxy.post(
                "/v1/chat/completions",
                json={"model": "m", "top_p": 0.9, "stream": False})
            assert captured["body"]["top_p"] == 0.9
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_bad_requests(tmp_path):
    async def main():
        backend, proxy, _ = await _with_servers(tmp_path, _ok_chat)
        try:
            async with proxy.post("/v1/chat/completions",
                                          data=b"") as r:
                assert r.status == 400
            async with proxy.post("/v1/chat/completions",
                                          data=b"not json") as r:
                assert r.status == 400
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_passthrough(tmp_path):
    async def main():
        backend, proxy, _ = await _with_servers(tmp_path, _ok_chat)
        try:
            async with proxy.get("/v1/models") as r:
                assert r.status == 200
                body = await r.json()
            assert body == {"data": ["m"], "object": "list"}
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_upstream_down_502(tmp_path):
    async def main():
        cfg = _cfg(tmp_path, "127.0.0.1:1")  # nothing listens there
        proxy = TestClient(TestServer(create_app(cfg)))
        await proxy.start_server()
        try:
            async with proxy.post(
                "/v1/chat/completions",
                json={"model": "m", "stream": False}) as r:
                assert r.status == 502
        finally:
            await proxy.close()
    asyncio.run(main())


def test_heartbeat_before_slow_answer(tmp_path):
    async def slow(request):
        await asyncio.sleep(1.5)
        return web.json_response(CHAT)

    async def main():
        backend, proxy, _ = await _with_servers(
            tmp_path, slow, {"KEEPALIVE_DELAY": "0.3",
                             "KEEPALIVE_INTERVAL": "0.3"})
        try:
            timestamps = []
            async with proxy.post(
                "/v1/chat/completions",
                json={"model": "m", "stream": True}) as r:
                assert r.status == 200
                loop = asyncio.get_event_loop()
                t0 = loop.time()
                async for chunk in r.content.iter_any():
                    timestamps.append((loop.time() - t0, chunk.decode()))
            first_gap = timestamps[0][0]
            assert first_gap < 1.4, "first heartbeat must arrive before the answer"
            first = json.loads(timestamps[0][1].split("data: ")[1].split("\n")[0])
            assert first["choices"][0]["delta"] == {}
            whole = "".join(t for _, t in timestamps)
            assert _content_of(whole) == "hello world"
            assert whole.rstrip().endswith("data: [DONE]")
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())


def test_upstream_error_relayed(tmp_path):
    async def err(request):
        return web.json_response({"error": "boom"}, status=500)

    async def main():
        backend, proxy, _ = await _with_servers(tmp_path, err)
        try:
            async with proxy.post(
                "/v1/chat/completions",
                json={"model": "m", "stream": False}) as r:
                assert r.status == 500
                body = await r.json()
            assert body["error"] == "boom"
        finally:
            await proxy.close(); await backend.close()
    asyncio.run(main())