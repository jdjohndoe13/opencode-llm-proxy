# LLM Proxy (OpenResty + Lua)

> **Python rewrite available.** The same proxy is implemented in Python
> (aiohttp) under `python/` — same behavior, same `.env` variables, same
> `logs/` file formats, port 9999. It exists to simplify debugging and
> maintenance. Run it in parallel with the live service:
>
> ```bash
> docker compose -f docker-compose.python.yaml up -d --build
> ```
>
> or locally (tests on an off-port so the live proxy stays untouched):
>
> ```bash
> cd python && pip install -r requirements.txt
> LLM_BACKEND=host.docker.internal:21025 PORT=9998 KEEPALIVE_DELAY=30 \
> KEEPALIVE_INTERVAL=5 UPSTREAM_TIMEOUT=600 UPSTREAM_RETRIES=1 \
> LOG_DIR=../logs python -m llm_proxy
> ```
>
> The OpenResty service defined by `docker-compose.yaml` remains the live
> one until the swap is approved. `PORT` is honored by the Python service
> only (the nginx config hardcodes 9999).

A small reverse proxy built on the `openresty/openresty:alpine` image that
sits in front of an LLM backend (e.g. a local vLLM server). It listens on
port **9999** and forwards everything to the backend configured via the
`LLM_BACKEND` variable in `.env`, with one special case:

For every request to **`/v1/completions`** or **`/v1/chat/completions`**
(a JSON object body), the proxy rewrites the request body by setting:

- `stream` → `false`
- `stream_options` → removed
- `top_p` → forced to the `TOP_P` value, but only when `TOP_P` is
  non-negative (the shipped default `-1` leaves the client's `top_p`
  untouched)

and then passes the modified body to the backend at the same path. The
backend therefore always responds with a single non-streaming JSON body.

If the client asked for `stream: true`, the proxy converts that
non-streaming response into a token-split SSE stream: `chat.completion`
bodies become `chat.completion.chunk` events (role, reasoning, content,
tool call, finish and usage deltas), `text_completion` bodies are split
into per-token `text` events, and the stream is terminated with
`data: [DONE]`. Clients that ask for `stream: false` still get the plain
JSON response. All other paths are proxied unchanged.

## Why: OpenCode compatibility

This proxy exists to make local vLLM backends usable with
[OpenCode](https://github.com/anomalyco/opencode), which hardcodes
`stream: true` for OpenAI-compatible providers (see
[anomalyco/opencode#785](https://github.com/anomalyco/opencode/issues/785)).
Some vLLM versions corrupt tool-call arguments in streaming mode, while the
same request answered with `stream: false` returns clean output — and the
workaround discussed in that issue is exactly what this proxy automates:
force `stream: false` upstream and re-serialize the non-streaming response
as an SSE stream for the client.

OpenCode additionally enforces timeouts on every provider request
(`packages/opencode/src/provider/provider.ts`): the response **headers**
must arrive within `headerTimeout` (default 300000 ms, i.e. 5 minutes;
`false` disables it in `opencode.jsonc`), and for `text/event-stream`
responses the gap between chunks must stay within `chunkTimeout` (default
300000 ms; `false` disables). There is also an overall `timeout` (default
300000 ms, `false` to disable) applied to the whole request — including
the streamed body, so for backends that generate for minutes on end it
should be raised or disabled. A local backend can easily stay silent
longer than these windows allow, and OpenCode then fails the request with
a timeout error. The proxy bridges the gap with the keep-alive mechanism
described in [How it works](#how-it-works): the SSE handshake is deferred
for `KEEPALIVE_DELAY` seconds (default 30); if the upstream response has
not fully arrived by then, the proxy commits to the stream — it sends
`200 OK` with `text/event-stream` (which satisfies `headerTimeout`) and
then an empty heartbeat chunk every `KEEPALIVE_INTERVAL` seconds (default
5, far below any `chunkTimeout`) until the real response is ready.
Upstream answers (errors included) that arrive within the delay window
are still relayed as plain HTTP responses.

## Files

```
.
├── .env.example                  # sample environment; copy to .env and edit
├── docker-compose.yaml           # service definition; values come from .env
├── restart.sh                    # full restart helper (down + recreate)
├── stall_stats.py                # summarize logs/stall.log
├── logs/                         # request/response dumps + stats (gitignored)
├── dashboard/                    # dashboard frontend (TS + Vite + pnpm,
│                                 #   built in Docker; see Dashboard below)
└── nginx/lua/
    ├── nginx.conf                # main config: Lua module, shared dict, resolver
    ├── default.conf.template     # server block (${...} substituted at startup)
    └── completions.lua           # body rewrite + SSE conversion handler
```

## Requirements

- Docker with Docker Compose
- The LLM backend must be reachable at the address set in `LLM_BACKEND`
- Python 3 (optional, only for `stall_stats.py`)

## Configuration

Docker Compose automatically reads `.env` from this directory and
substitutes its values into `docker-compose.yaml`. Copy the sample file and
edit it:

```bash
cp .env.example .env
```

| Variable | Meaning |
| --- | --- |
| `LLM_BACKEND` | Backend `host:port` (e.g. `host.docker.internal:21025`); used as the upstream address and as the `Host` header sent upstream |
| `TOP_P` | `top_p` forced onto rewritten request bodies; a negative value (default `-1`) leaves the client's `top_p` untouched |
| `TEMP_CHECK_ENABLED` | `1` or `true` (case-insensitive) arms the GPU temperature guard; anything else (or unset) disables it and every other `TEMP_CHECK_*` variable is ignored. The dashboard can additionally switch the guard off at runtime (re-arms on restart) |
| `TEMP_CHECK_FAIL_OPEN` | Python service only. When the monitor endpoint is unreachable: `1`/`true` forwards the request as if the temperatures were OK; `0` (default) keeps the conservative stall (requests wait until the monitor answers again) |
| `TEMP_CHECK_ENDPOINT` | Full URL of a monitor returning a JSON object of GPU temperatures, e.g. `http://host.docker.internal:18082/temperatures` → `{"gpu_0":48,...}` |
| `TEMP_CHECK_STALL_ABOVE` | Requests are stalled while any GPU temperature is above this value. Editable at runtime from the dashboard (resets to this value on restart) |
| `TEMP_CHECK_RESUME_BELOW` | Stalled requests resume once every temperature is below this value (hysteresis). Editable at runtime from the dashboard (resets to this value on restart) |
| `TEMP_CHECK_POLL_INTERVAL` | Seconds between monitor polls (shared across all requests/workers) |
| `TEMP_CHECK_CACHE_TTL` | Seconds a fetched temperature snapshot stays valid in the shared cache |
| `TEMP_CHECK_HTTP_TIMEOUT` | Millisecond timeout for monitor HTTP calls |
| `TEMP_CHECK_STALL_LOG` | Path (inside the container) where stall records are written |
| `KEEPALIVE_DELAY` | Seconds after the request was received before the SSE handshake and the first heartbeat are sent |
| `KEEPALIVE_INTERVAL` | Seconds between heartbeat chunks while the upstream is still generating |
| `UPSTREAM_TIMEOUT` | Seconds each upstream attempt (connect + send + full response) has to answer; when it expires without a (complete) response, the connection is dropped |
| `UPSTREAM_RETRIES` | How many times the identical request is resent after such a timeout (a request is sent at most `UPSTREAM_RETRIES + 1` times) |
| `CONCURRENCY_LIMIT` | Maximum number of requests allowed at the LLM server at once; `0` (default) means unlimited. Requests that arrive while the cap is full are held in arrival order (shown as `queued` on the dashboard) and the oldest is released as soon as a running request finishes. Editable at runtime from the dashboard (resets to this value on restart). A slot is taken only for the upstream exchange, so requests held by the pause gate or the temperature guard do not consume one |
| `TOOL_NAME_GUARD` | `remap` (default), `strip` or `off`. When tools are defined, markup-style tool calls in the response (including the `antml:` namespace) are converted to real `tool_calls` and their names validated against the request's tool list: `remap` fixes a hallucinated name when a unique allowed candidate exists (edit distance ≤ 2, or a unique shared intent token ≥ 5 chars within the same `server_` prefix), `strip` always drops unknown names. Dropped calls are replaced by a corrective note in the assistant content tagged `[tool-name-fixer]`; every decision is logged to `logs/toolname_fixes.jsonl` |
| `TOOL_NAME_RETRY` | `1` (default) or `0`. When `1` and the guard would strip anything, the identical request is silently resent once (only empty heartbeat chunks have reached the client at that point) and the fresh sample is adopted if it comes back as a clean chat completion; otherwise the original response is demoted as above. Decisions appear in `logs/*.jsonl` stats (`toolname_resend`) and nginx warnings |

When `TEMP_CHECK_ENABLED` is `1`/`true`, the other `TEMP_CHECK_*` variables
are required — the container refuses to start if any is missing. `LLM_BACKEND`,
`KEEPALIVE_DELAY`, `KEEPALIVE_INTERVAL`, `UPSTREAM_TIMEOUT` and
`UPSTREAM_RETRIES` are always required.

To point the proxy at a different backend, edit `LLM_BACKEND` in `.env` and
restart (see below).

## Usage

```bash
docker compose up -d
```

The proxy is then available at `http://localhost:9999`.

Example (chat completions):

```bash
curl http://localhost:9999/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-3.5","messages":[{"role":"user","content":"Hello"}],"stream":true}'
```

Example (completions):

```bash
curl http://localhost:9999/v1/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-3.5","prompt":"Hello","top_p":0.3,"stream":true}'
```

The backend receives the body with `stream: false` (and `top_p` forced to
the `TOP_P` value when `TOP_P` is non-negative) and responds non-streaming.
When the client sent `stream: true`, the response is
returned as an SSE stream of `chat.completion.chunk` / `text_completion`
events (one `data:` line per delta), terminated by `data: [DONE]`.

To apply configuration changes or restart the proxy:

```bash
docker compose up -d --force-recreate
```

or, for a full restart (`down -v --remove-orphans`, then recreate):

```bash
./restart.sh
```

Stop the proxy:

```bash
docker compose down
```

## How it works

The stock `openresty/openresty:alpine` image ships ngx_lua, so no custom
image is required — the config mounts the Lua script and starts OpenResty in
the foreground.

1. `nginx/lua/nginx.conf` sets the Lua module path
   (`lua_package_path '/etc/nginx/lua/?.lua;;'`), the `temps` shared dict
   (GPU temperature snapshots shared across all workers/requests), and the
   Docker embedded DNS resolver (`127.0.0.11`) for cosocket hostname
   lookups.
2. At container startup the compose `command` validates the required
   variables and `sed`-substitutes `${LLM_BACKEND}`, the `TEMP_CHECK_*`
   variables and `KEEPALIVE_DELAY` / `KEEPALIVE_INTERVAL` into
   `nginx/lua/default.conf.template`, writing the result to
   `/etc/nginx/conf.d/default.conf`.
3. `default.conf` routes `/v1/completions` and `/v1/chat/completions`
   (regex `^/v1/(chat/)?completions$`) to the `completions.lua` handler
   (`access_by_lua_block` / `content_by_lua_block` / `log_by_lua_block`);
   every other path is proxied transparently to `http://${LLM_BACKEND}` with
   one-hour connect/send/read timeouts and `X-Forwarded-*` headers.
4. The handler reads the request body (`ngx.req.read_body`), parses it as
   JSON, forces `stream: false`, drops `stream_options`, forces `top_p` to
   the `TOP_P` value when `TOP_P` is non-negative, and re-serializes it.
   The original (unmodified) body is dumped to
   `logs/<unix-ms>.req.json`.
5. A client-disconnect handler is registered via `ngx.on_abort` (armed by
   `lua_check_client_abort on`) before the backend call: if the client
   aborts while the upstream call is in flight, the handler closes the
   cosocket so the backend (vLLM) aborts generation instead of running to
   completion, and the request ends with `444`.
6. If the temperature guard is enabled, the request is held (with heartbeat
   chunks going out for streaming clients) until every GPU temperature is
   below `TEMP_CHECK_RESUME_BELOW`. Snapshots are fetched from
   `TEMP_CHECK_ENDPOINT` at most once per `TEMP_CHECK_POLL_INTERVAL` and
   cached for `TEMP_CHECK_CACHE_TTL` seconds; if the monitor cannot be
   reached, the request is conservatively stalled.
7. The modified body is sent to the backend over a cosocket, carrying the
   original request headers except `Host`, `Content-Length`, `Connection`
   and `Accept-Encoding` (`Host` is set to `LLM_BACKEND`, identity encoding
   is forced). Each attempt gets `UPSTREAM_TIMEOUT` seconds to answer
   (connect + send + full buffered response); if the upstream has not
   finished responding within it, the cosocket is dropped and the identical
   request is resent, up to `UPSTREAM_RETRIES` times, with the empty SSE
   heartbeats for streaming clients continuing meanwhile. The upstream
   response is buffered in full, dumped to `logs/<unix-ms>.resp.json`, and
   its status, `Content-Type` and `Cache-Control` are relayed to the client.
8. When the client asked for `stream: true`, the SSE handshake is deferred
   until `KEEPALIVE_DELAY` seconds after the request was received; empty
   heartbeat chunks are then emitted every `KEEPALIVE_INTERVAL` seconds
   while the backend is still generating. This keeps clients whose
   first-byte timeout is shorter than the backend latency from aborting the
   stream, while fast upstream errors (answered within the delay window)
   still come back as plain HTTP errors. Once the upstream response
   arrives, `chat.completion` bodies are re-emitted as
   `chat.completion.chunk` deltas (role, reasoning, content, tool calls,
   finish, usage) and `text_completion` bodies as per-token `text` events,
   then `data: [DONE]`. Content is chunked on word boundaries (~16 chars),
   always on UTF-8 character boundaries, so concatenating the deltas
   reproduces the original text exactly. The stream is flushed with
   `ngx.print`/`ngx.flush`, so the response is chunked (no
   `Content-Length`).
9. In the log phase, the handler appends one JSON line per request that was
   actually forwarded to the upstream to `logs/stats.jsonl` (model, token
   usage, upstream status, stall time and timings) and, when the
   temperature guard is enabled, a stall record to the log configured via
   `TEMP_CHECK_STALL_LOG`.

## Request / response dumps

For every request to `/v1/completions` or `/v1/chat/completions`, the
handler writes the original request body and the backend response body to
`logs/` (which is bind-mounted into the container at
`/var/log/nginx/completions`):

```
logs/<unix-ms>.req.json     # original JSON body as sent by the client
logs/<unix-ms>.resp.json    # JSON body as returned by the backend
```

Both files of one exchange share the same millisecond-precision Unix
timestamp in their name. The `.gitignore` excludes the dump files. Dump
write failures are logged to nginx's error log and never affect the proxied
response.

## Dashboard: pause / resume (Python service)

The Python service serves a small ops dashboard at
`http://localhost:9999/dashboard` with a pause/resume switch:

- **Pause** — in-flight requests keep being served normally. Every *new*
  `/v1/completions` / `/v1/chat/completions` request is held without ever
  touching the backend: streaming clients get the identical deferred SSE
  handshake and fake empty heartbeat chunks (as if the upstream were
  slow), non-streaming clients just receive no reply until the proxy is
  resumed or the client's own timeout fires.
- **Resume** — the held requests are released and proceed normally
  (temp guard, then upstream).

The intended workflow: pause the proxy, reboot the LLM server, resume
when the new server is up. Clients only ever see "the server is slow",
never "the server is down". (Requests already in flight when the backend
dies still fail — nothing the proxy can do about that.) The pause state
is in-memory only: a proxy restart always boots running.

The dashboard shows the pause state, live in-flight requests (model,
stream, duration with the held/processed split, waiting/paused/stalled/queued/upstream), the recent pause/resume
events and the paginated request history read from `logs/stats.jsonl`
(newest first; `paused_seconds`, `queued_seconds` and `total_seconds` are
recorded per request). Each duration cell reads
`8.0s (5.0q + 3.0p)` — total wall-clock, of which 5.0s held by the pause
gate / temperature guard / concurrency queue and 3.0s spent at the LLM
server; the breakdown is omitted when nothing was held.

API (plain JSON, no auth — localhost tool):

```
GET  /api/dashboard/state              paused, in-flight, keepalive, temp guard, events
POST /api/dashboard/pause              pause the proxy
POST /api/dashboard/resume             resume the proxy
POST /api/dashboard/tempguard          {"enabled": true|false} — runtime switch
                                       for the GPU temperature guard (the
                                       guard re-arms on proxy restart)
POST /api/dashboard/tempguard/thresholds  {"stall_above":n,"resume_below":n}
                                       (either key alone is enough) or
                                       {"reset":true} — live hysteresis pair,
                                       also reset on proxy restart; 400 when
                                       the pair is invalid or the guard is
                                       disarmed by config
POST /api/dashboard/concurrency        {"limit":n} (0 = unlimited) or
                                       {"reset":true} — live concurrent-
                                       request cap, also reset on proxy
                                       restart; raising it admits queued
                                       requests, lowering it never disturbs
                                       requests already running; 400 when
                                       the value is invalid
GET  /api/dashboard/history?page=1&size=20   stats.jsonl tail, newest first
GET  /api/dashboard/healthz            {ok:true} — proxy vs backend
```

### The frontend (TypeScript + Vite + pnpm, built in Docker)

Source lives in `dashboard/` (no framework, plain TS). Node, pnpm and
Vite are never needed on the host — the proxy image build (`python/
Dockerfile`) starts from `node:22-alpine`, runs `pnpm install
--frozen-lockfile && pnpm build` in Docker and copies the Vite output
into the Python image, so `docker compose up -d --build` (or
`./restart.sh`) is all it takes. The dashboard is served straight from
the proxy — no extra service, no volume.

For working on the frontend itself:

```bash
cd dashboard
build.cmd        # Windows: build in Docker, extract dist/ (or ./build.sh)
```

`build.cmd` only regenerates `dashboard/dist/` (used for local
non-Docker runs); it is not needed for the normal image build.

## Stall statistics

`stall_stats.py` summarizes the stall log written by the temperature guard:

```bash
python stall_stats.py                    # human-readable summary of logs/stall.log
python stall_stats.py logs/stall.log -j  # compact JSON summary
```

It reports total request time, total stall time (time requests spent
waiting for the GPUs to cool down), and how many requests were stalled or
aborted.

## Notes / limitations

- Only `/v1/completions` and `/v1/chat/completions` are rewritten; every
  other path is proxied as-is. The rewrite matches the regex
  `^/v1/(chat/)?completions$`, so other `/v1/.../completions` endpoints
  would need the pattern extended.
- The backend must answer with a normal (non-streaming) JSON response — the
  response is buffered in full by the Lua handler before being sent to the
  client, so SSE/streaming from the upstream is not supported on the
  rewritten endpoints.
- `stream` is always forced to `false`. `top_p` can be forced to a fixed
  value via `TOP_P`; with the default `-1` clients control `top_p`.
- The SSE stream returned to streaming clients is a token-split
  approximation of the upstream format: there is no realtime
  token-by-token timing while the response is being generated (empty
  heartbeat chunks are sent instead).
- Tool calls are preserved: when the non-streaming response carries
  `message.tool_calls`, or contains tool-call markup (`<tool_calls>` /
  `<invoke name=...>`, also in the `DSML` special-token form
  `<\uFF5CDSML\uFF5C...>` that the vLLM backend leaves inside `reasoning`
  when `stream=false`), the converter strips the markup and emits
  OpenAI-style `delta.tool_calls` events with `finish_reason: "tool_calls"`,
  matching what a real streaming response would look like.
- Invalid or empty JSON bodies on a rewritten endpoint return `400`.
- Request bodies are kept fully in memory (`client_max_body_size` /
  `client_body_buffer_size` 100m), so very large requests consume memory.
- The `Host` header sent to the backend is the `LLM_BACKEND` address (not
  the original client `Host`), which is fine for LLM backends that don't
  use virtual hosting.

## License

MIT — see [LICENSE](LICENSE).
