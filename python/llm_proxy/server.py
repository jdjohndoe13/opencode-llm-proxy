"""aiohttp server: the Python port of nginx/lua/completions.lua +
default.conf.template.

Behavioral contract (see .slim/deepwork/recipes.md for the aiohttp
patterns used):
- /v1/completions and /v1/chat/completions: rewrite the body
  (stream=false, stream_options dropped, top_p forced when TOP_P >= 0),
  forward to the backend, and re-serialize the non-streaming answer as an
  SSE stream when the client asked for stream:true.
- The SSE handshake is deferred until KEEPALIVE_DELAY seconds after the
  request was received; upstream answers inside that window are relayed
  as plain HTTP.
- Every upstream attempt gets UPSTREAM_TIMEOUT seconds (connect + send +
  full read); on timeout the identical request is resent, up to
  UPSTREAM_RETRIES times.
- A client disconnect cancels the handler (handler_cancellation) and
  thereby the upstream request, aborting backend generation.
- All other paths are proxied transparently.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

import aiohttp
from aiohttp import web

from . import dashboard, dumps, sse as sse_mod
from .config import Config, load
from .dumps import dump, dump_ts
from .inflight import InFlight
from .limiter import ConcurrencyLimiter
from .markup import json_encode
from .pause import PauseGate
from .toolguard import ToolNameGuard, allowed_names
from .tempguard import TempGuard

log = logging.getLogger("llm_proxy")

REWRITTEN_PATH = "/v1/chat/completions"
COMPLETIONS_PATH = "/v1/completions"
BACKEND_PATHS = (REWRITTEN_PATH, COMPLETIONS_PATH)

HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate",
              "proxy-authorization", "te", "trailer", "trailers",
              "transfer-encoding", "upgrade"}


class UpstreamError(Exception):
    """Fatal upstream failure; the message is relayed to the client."""


class StreamCommit:
    """Deferred SSE handshake + heartbeat state machine (the Lua
    send_keepalive)."""

    def __init__(self, request, cfg, want_stream, req_done, is_chat,
                 cid, created, model):
        self.request = request
        self.cfg = cfg
        self.want_stream = want_stream
        self.req_done = req_done
        self.is_chat = is_chat
        self.cid = cid
        self.created = created
        self.model = model
        self.sse_started = False
        self.last_keepalive: float | None = None
        self.resp: web.StreamResponse | None = None

    def _due(self, now) -> bool:
        if not self.want_stream:
            return False
        if not self.sse_started:
            return now - self.req_done >= self.cfg.keepalive_delay
        return now - (self.last_keepalive or self.req_done) >= self.cfg.keepalive_interval

    def heartbeat(self) -> bytes:
        return ("data: " + sse_mod.keepalive_chunk(
            self.is_chat, self.cid, self.created, self.model)
                + "\n\n").encode("utf-8")

    async def on_wait(self) -> None:
        """Called periodically while the upstream is still pending: commits
        the deferred SSE handshake once KEEPALIVE_DELAY has elapsed and then
        emits one heartbeat per KEEPALIVE_INTERVAL. Mirrors send_keepalive."""
        if not self.want_stream:
            return
        now = time.time()
        if not self.sse_started:
            if now - self.req_done < self.cfg.keepalive_delay:
                return
            resp = await _commit_sse(self.request, self)
            self.resp = resp
            self.last_keepalive = now
            await resp.write(self.heartbeat())
            return
        if now - (self.last_keepalive or self.req_done) < self.cfg.keepalive_interval:
            return
        self.last_keepalive = now
        if self.resp is not None:
            await self.resp.write(self.heartbeat())


def _fwd_headers(request: web.Request, backend: str) -> dict[str, str]:
    """Headers for the rewritten upstream call: everything except Host,
    Content-Length, Connection and Accept-Encoding; Host set to the backend,
    identity encoding forced. Mirrors the Lua header build."""
    headers = {}
    for k, v in request.headers.items():
        lk = k.lower()
        if lk in ("host", "content-length", "connection", "accept-encoding"):
            continue
        headers[k] = v
    headers["Host"] = backend
    headers["Accept-Encoding"] = "identity"
    return headers


async def fetch_upstream(app: web.Application, ctx: dict, body: bytes,
                         path: str,
                         headers: dict[str, str]) -> tuple[int, dict, bytes]:
    """Send the (rewritten) request to the backend, buffering the full
    response. Each attempt gets UPSTREAM_TIMEOUT seconds; on that deadline
    the identical request is resent, up to UPSTREAM_RETRIES times. Returns
    (status, headers, body). Raises UpstreamError for fatal failures."""
    cfg: Config = app["cfg"]
    session: aiohttp.ClientSession = app["session"]
    url = f"http://{cfg.llm_backend}{path}"
    attempts = cfg.upstream_retries + 1
    timeout = aiohttp.ClientTimeout(
        total=cfg.upstream_timeout, sock_connect=10,
        sock_read=cfg.upstream_timeout)

    for attempt in range(1, attempts + 1):
        try:
            async with session.post(url, data=body, headers=headers,
                                    timeout=timeout,
                                    allow_redirects=False) as resp:
                raw = await resp.read()
                return (resp.status,
                        {k.lower(): v for k, v in resp.headers.items()},
                        raw)
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            # A connect-phase timeout is fatal (the Lua only retries the
            # response deadline, never a connect failure).
            if (isinstance(err, TimeoutError)
                    and not isinstance(err, aiohttp.ConnectionTimeoutError)
                    and attempt < attempts):
                log.warning("upstream did not respond within %ss, dropping the "
                            "connection and resending the request (attempt %s "
                            "of %s)", cfg.upstream_timeout, attempt + 1, attempts)
                ctx["_upstream_retries"] = ctx.get("_upstream_retries", 0) + 1
                continue
            raise UpstreamError(f"upstream error: {err}") from err
    raise UpstreamError("upstream error: exhausted retries")


async def _drive_fetch(app, ctx, body, path, headers, commit):
    """Run fetch_upstream as a task while pumping the deferred SSE handshake
    and heartbeats (the Lua's recv pollers call send_keepalive on every
    receive timeout)."""
    task = asyncio.create_task(fetch_upstream(app, ctx, body, path, headers))
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=0.25)
            if task in done:
                break
            await commit.on_wait()
        return task.result()
    finally:
        if not task.done():
            task.cancel()


async def _send_sse(resp: web.StreamResponse, payload: str) -> None:
    await resp.write(("data: " + payload + "\n\n").encode("utf-8"))


async def _write_heartbeat(commit: StreamCommit) -> None:
    assert commit.resp is not None
    await commit.resp.write(commit.heartbeat())


async def _emit_sse(resp: web.StreamResponse, payloads: list[str]) -> None:
    for p in payloads:
        await _send_sse(resp, p)


async def _commit_sse(request: web.Request, commit: StreamCommit) -> web.StreamResponse:
    resp = web.StreamResponse(status=200, headers={
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
    })
    await resp.prepare(request)
    commit.sse_started = True
    commit.resp = resp
    return resp


async def _respond_plain(request: web.Request, status: int, headers: dict,
                         body: bytes) -> web.Response:
    out = web.Response(status=status, body=body)
    ct = headers.get("content-type") or "application/json"
    out.headers["Content-Type"] = ct
    if headers.get("cache-control"):
        out.headers["Cache-Control"] = headers["cache-control"]
    return out


async def completions(request: web.Request) -> web.StreamResponse:
    cfg: Config = request.app["cfg"]
    guard: ToolNameGuard = request.app["guard"]
    ctx: dict = {"_start": time.time(), "_uri": request.path}
    request["_ctx"] = ctx  # found by stats_middleware in the log phase

    raw = await request.read()
    req_done = time.time()
    ctx["_req_done"] = req_done
    if not raw:
        return web.Response(status=400, text="empty request body")

    ts = dump_ts()
    dump(cfg.log_dir, ts, "req", raw)

    try:
        obj = json.loads(raw)
    except ValueError:
        obj = None
    if not isinstance(obj, dict):
        return web.Response(status=400, text="invalid JSON in request body")

    want_stream = obj.get("stream") is True
    if cfg.top_p >= 0:
        obj["top_p"] = cfg.top_p
    obj["stream"] = False
    obj.pop("stream_options", None)
    new_body = json_encode(obj).encode("utf-8")

    is_chat = "/chat/" in request.path
    ctx["_req_model"] = obj.get("model") if isinstance(obj.get("model"), str) else None
    tool_names = allowed_names(obj)

    commit = StreamCommit(
        request, cfg, want_stream, req_done, is_chat,
        cid=obj.get("id") if isinstance(obj.get("id"), str) and obj.get("id") else "chatcmpl-keepalive",
        created=int(time.time()),
        model=obj.get("model") if isinstance(obj.get("model"), str) else "unknown",
    )

    # In-flight registry (dashboard): one record per forwarded request,
    # removed in stats_middleware's finally (covers every exit path,
    # including cancellation).
    inflight: InFlight = request.app["inflight"]
    rec = inflight.add(
        str(ts),
        model=ctx["_req_model"],
        path=request.path,
        stream=want_stream)
    ctx["_inflight"] = rec
    # Kill handles for the dashboard's "drop all" (see inflight.drop_all):
    # the handler task + the client transport. The private side-map keeps
    # them out of the JSON the dashboard polls. Keyed by the record's own
    # id — add() suffixes it so same-millisecond requests cannot collide.
    inflight.register_kill(rec["id"], ctx, request.transport,
                           asyncio.current_task())

    # Manual pause gate (before the temp guard and the upstream): while
    # paused the upstream is never touched — no connect attempts at all.
    # Streaming clients keep the identical deferred handshake + heartbeat
    # behavior (on_wait pumps the very same StreamCommit), non-stream
    # clients receive no reply bytes and just hang until resumed or until
    # their own timeout kills them. On resume the request proceeds
    # normally (temp guard -> fetch).
    pause_gate: PauseGate = request.app["pause_gate"]

    async def _on_wait_pause() -> None:
        if rec["state"] != "paused":
            inflight.set_state(rec, "paused")
        await commit.on_wait()

    if not await pause_gate.wait_until_resumed(ctx, _on_wait_pause):
        log.info("client aborted while the proxy was paused")
        return web.Response(status=444)  # nginx "no response" analog
    if rec["state"] == "paused":
        inflight.set_state(rec, "waiting")

    # Cool-down guard (stalls with heartbeats for streaming clients).
    temp_guard: TempGuard = request.app["temp_guard"]

    async def _on_wait_guard() -> None:
        if ctx.get("_stall_start") and not ctx.get("_stall_end"):
            if rec["state"] != "stalled":
                inflight.set_state(rec, "stalled")
        elif rec["state"] == "stalled":
            inflight.set_state(rec, "waiting")
        await commit.on_wait()

    if not await temp_guard.wait_for_cool_down(ctx, _on_wait_guard):
        log.info("client aborted while stalling for cool-down")
        return web.Response(status=444)  # nginx "no response" analog

    # Concurrency limiter (last gate before the upstream). A slot is held
    # only for the upstream exchange, so a request still waiting on the
    # pause gate or the temperature guard costs nothing. Queued requests get
    # the same deferred handshake + heartbeat treatment as the other gates.
    limiter: ConcurrencyLimiter = request.app["limiter"]

    async def _on_wait_limit() -> None:
        if rec["state"] != "queued":
            inflight.set_state(rec, "queued")
        await commit.on_wait()

    if not await limiter.acquire(ctx, _on_wait_limit):
        log.info("client aborted while queued for a concurrency slot")
        return web.Response(status=444)  # nginx "no response" analog
    if rec["state"] == "queued":
        inflight.set_state(rec, "waiting")

    ctx["_sent_at"] = time.time()
    inflight.set_state(rec, "upstream")

    path = request.path
    if request.query_string:
        path = path + "?" + request.query_string
    headers = _fwd_headers(request, cfg.llm_backend)

    # Drive the upstream exchange with the deferred handshake: while the
    # attempt is in flight, on_wait() commits the SSE handshake once
    # KEEPALIVE_DELAY has elapsed and then emits one heartbeat per
    # KEEPALIVE_INTERVAL. Inside the delay window the answer is still
    # relayable as plain HTTP.
    try:
        status, up_headers, body = await _drive_fetch(
            request.app, ctx, new_body, path, headers, commit)
    except UpstreamError as err:
        log.warning("upstream error: %s", err)
        if commit.sse_started:
            await _send_sse(commit.resp, sse_mod.sse_error(str(err)))
            await _send_sse(commit.resp, "[DONE]")
            return commit.resp
        return web.Response(status=502, text=str(err))
    except asyncio.CancelledError:
        # client went away (handler_cancellation): the upstream fetch dies
        # with us, which is exactly the vLLM-generation-abort behavior.
        log.info("client aborted, closing upstream connection")
        raise

    ctx["_upstream_status"] = status
    ctx["_upstream_done"] = time.time()
    dump(cfg.log_dir, ts, "resp", body)

    jobj = None
    if status == 200 and body:
        try:
            decoded = json.loads(body)
            if isinstance(decoded, dict):
                jobj = decoded
                ctx["_resp_usage"] = decoded.get("usage")
                ctx["_resp_model"] = decoded.get("model")
        except ValueError:
            pass

    # Tool-name guard silent self-heal: resend the identical request once
    # when the guard would strip anything, and adopt the fresh sample when
    # it comes back as a clean chat completion.
    if (guard.retry_enabled and jobj is not None and want_stream
            and tool_names and len(tool_names) > 0):
        from .sse import _probe_strips
        nstrip = _probe_strips(jobj, tool_names, guard)
        if nstrip > 0:
            log.warning("tool-name guard: may strip %s call(s), resending the "
                        "request once", nstrip)
            ctx["_toolname_resend"] = ctx.get("_toolname_resend", 0) + 1
            st2, hd2, b2 = None, {}, b""
            try:
                # Pump heartbeats during the resend too (Lua: the resend's
                # recv pollers keep calling send_keepalive).
                st2, hd2, b2 = await _drive_fetch(
                    request.app, ctx, new_body, path, headers, commit)
            except UpstreamError:
                st2 = None
            if st2 is not None:
                dump(cfg.log_dir, ts, "resp.2", b2)
                if st2 == 200 and b2:
                    try:
                        dec2 = json.loads(b2)
                    except ValueError:
                        dec2 = None
                    if (isinstance(dec2, dict)
                            and isinstance(dec2.get("choices"), list)
                            and dec2["choices"]):
                        body, status, headers, jobj = b2, st2, hd2, dec2
                        ctx["_upstream_status"] = st2
                        ctx["_resp_usage"] = dec2.get("usage")
                        ctx["_resp_model"] = dec2.get("model")
                    else:
                        log.warning("tool-name retry: non-chat response, "
                                    "keeping original")
                else:
                    log.warning("tool-name retry: status %s, keeping original", st2)

    # Response dispatch (mirrors the Lua final block).
    if want_stream and commit.sse_started:
        resp = commit.resp
        if status != 200:
            if not body:
                await _send_sse(resp, sse_mod.sse_error(
                    f"upstream returned status {status}"))
            else:
                await _send_sse(resp, body.decode("utf-8", "replace"))
            await _send_sse(resp, "[DONE]")
            ctx["_final_status"] = 200
            return resp
        if not body:
            await _send_sse(resp, "[DONE]")
            ctx["_final_status"] = 200
            return resp
    else:
        if not want_stream or status != 200:
            ctx["_final_status"] = status
            return await _respond_plain(request, status, up_headers, body)
        # want_stream, answer arrived inside the delay window: plain HTTP
        # relay of the non-streaming body is NOT what the client asked for;
        # the Lua still re-streams here (handshake not yet sent).
        resp = await _commit_sse(request, commit)
        if not body:
            await _send_sse(resp, "[DONE]")
            ctx["_final_status"] = 200
            return resp

    if jobj is None:
        await _send_sse(resp, body.decode("utf-8", "replace"))
        await _send_sse(resp, "[DONE]")
        ctx["_final_status"] = 200
        return resp

    try:
        if jobj.get("object") == "chat.completion" or "/chat/" in path:
            payloads, stats = sse_mod.emit_chat_stream(jobj, tool_names, guard)
            ctx["_toolname_remap"] = ctx.get("_toolname_remap", 0) + stats["remap"]
            ctx["_toolname_strip"] = ctx.get("_toolname_strip", 0) + stats["strip"]
        else:
            payloads = sse_mod.emit_text_stream(jobj)
        await _emit_sse(resp, payloads)
    except asyncio.CancelledError:
        raise
    except Exception as err:  # mirrors the Lua pcall around emission
        log.warning("SSE emission failed (client aborted?): %s", err)
    ctx["_final_status"] = 200
    return resp


async def passthrough(request: web.Request) -> web.StreamResponse:
    """Transparent proxy for every other path (the Lua `location /` with
    proxy_pass). Host rewritten to the backend, X-Forwarded-* added, one
    hour connect/send/read timeouts."""
    cfg: Config = request.app["cfg"]
    session: aiohttp.ClientSession = request.app["session"]
    url = f"http://{cfg.llm_backend}{request.path}"
    if request.query_string:
        url += "?" + request.query_string

    headers = {}
    for k, v in request.headers.items():
        lk = k.lower()
        if lk in HOP_BY_HOP or lk in ("host", "content-length"):
            continue
        headers[k] = v
    headers["Host"] = cfg.llm_backend
    fwd = request.headers.get("X-Forwarded-For")
    peer = request.remote or ""
    headers["X-Forwarded-For"] = f"{fwd}, {peer}" if fwd else peer
    headers["X-Forwarded-Proto"] = request.scheme

    body = await request.read()
    timeout = aiohttp.ClientTimeout(
        total=None, sock_connect=10, sock_read=3600)
    try:
        async with session.request(
            request.method, url, headers=headers, data=body,
            allow_redirects=False, auto_decompress=False, timeout=timeout,
        ) as up:
            out = web.StreamResponse(status=up.status)
            for k, v in up.headers.items():
                lk = k.lower()
                if lk in HOP_BY_HOP or lk in ("content-length", "content-encoding"):
                    continue
                out.headers[k] = v
            await out.prepare(request)
            try:
                async for chunk in up.content.iter_any():
                    await out.write(chunk)
            except asyncio.CancelledError:
                raise
            except (ConnectionResetError, OSError):
                pass
            try:
                await out.write_eof()
            except (ConnectionResetError, OSError):
                pass
            return out
    except asyncio.CancelledError:
        raise
    except (aiohttp.ClientError, asyncio.TimeoutError) as err:
        return web.Response(status=502, text=f"upstream error: {err}")


async def _health(request: web.Request) -> web.Response:
    return web.Response(text="ok")


@web.middleware
async def stats_middleware(request, handler):
    """Per-request context + the log-phase stats/stall writers (the Lua
    log_by_lua_block — only the completions location logged, so we only
    write when the handler registered its context)."""
    ctx = request.get("_ctx")
    try:
        resp = await handler(request)
    except asyncio.CancelledError:
        # Set the flag before re-raising so a handler still suspended inside
        # a wait loop (pause gate / temp guard) can observe ctx["_aborted"]
        # on its next tick and unwind cleanly instead of hanging forever.
        ctx = request.get("_ctx") or {}
        ctx["_aborted"] = True
        ctx.setdefault("_final_status", 444)  # the Lua's 444 on abort
        raise
    else:
        ctx = request.get("_ctx")
        if ctx is not None and "_final_status" not in ctx:
            ctx["_final_status"] = getattr(resp, "status", 0)
    finally:
        ctx = request.get("_ctx")
        if ctx is not None:
            ctx["_ended"] = time.time()
            cfg = request.app["cfg"]
            # Return the concurrency slot first: the release admits the next
            # queued request, and that request's state change should not
            # trail our own cleanup.
            if ctx.get("_slot_held"):
                try:
                    request.app["limiter"].release()
                except Exception:
                    log.exception("concurrency slot release failed")
            rec = ctx.get("_inflight")
            if rec is not None:
                try:
                    request.app["inflight"].remove(rec["id"])
                except Exception:
                    log.exception("in-flight registry cleanup failed")
            try:
                dumps.write_stats(cfg, ctx)
                dumps.write_stall(cfg, ctx)
            except Exception:
                log.exception("stats logging failed")
    return resp


def create_app(cfg: Config) -> web.Application:
    app = web.Application(middlewares=[stats_middleware],
                          client_max_size=100 * 1024 * 1024)
    app["cfg"] = cfg
    app["guard"] = ToolNameGuard(cfg.tool_name_guard, cfg.tool_name_retry,
                                 cfg.toolname_log)
    app["limiter"] = ConcurrencyLimiter(cfg)

    async def _session_ctx(app):
        # force_close mirrors the Lua cosocket model: one fresh connection
        # per request. Pooled keep-alives race the backend's idle-close and
        # surface as instant 502s (ServerDisconnectedError on a dead socket).
        app["session"] = aiohttp.ClientSession(
            auto_decompress=False,
            connector=aiohttp.TCPConnector(force_close=True))
        yield
        await app["session"].close()

    async def _temp_ctx(app):
        app["temp_guard"] = TempGuard(cfg, app.get("session"))

    app.cleanup_ctx.append(_session_ctx)
    app.on_startup.append(_temp_ctx)

    app.router.add_route("*", "/health", _health)
    app.router.add_route("*", "/v1/completions", completions)
    app.router.add_route("*", "/v1/chat/completions", completions)
    # Dashboard API + static UI must be registered BEFORE the catch-all.
    dashboard.register(app)
    app.router.add_route("*", "/{tail:.*}", passthrough)
    return app


def run(cfg: Config | None = None) -> None:
    if cfg is None:
        cfg = load()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    web.run_app(create_app(cfg), host="0.0.0.0", port=cfg.port,
                handler_cancellation=True, shutdown_timeout=5)
