# llm-proxy dashboard

Single-page ops console for the LLM proxy, served by the proxy itself at
`/dashboard`. Plain TypeScript + Vite — no framework, no CDN fetches, fully
offline-safe. Dark, high-density, monospace; hand-rolled CSS in one file.

## Build (Docker only — no node/pnpm needed on the host)

From this directory:

```cmd
build.cmd
```

which is exactly:

```cmd
docker build -t llm-proxy-dashboard .
docker create --name tmp llm-proxy-dashboard
docker cp tmp:/app/dist ./dist
docker rm -f tmp
```

`docker build --output type=local,dest=dist .` works too — the release stage
carries the dist at the filesystem root (note: that flow also drops a
duplicate `app/dist` mirror alongside the files; `build.cmd` is the cleaner
path).

The resulting `dist/` is only needed for local (non-Docker) runs of the
proxy: `python/llm_proxy/static/` is its fallback home. The normal
deployment path does not need it at all — the proxy image build
(`python/Dockerfile`, repo-root context) compiles this frontend itself
(node:22-alpine + corepack pnpm) and bakes the output into the image.

## Dev mode (only if you happen to have node + pnpm)

```sh
pnpm install
pnpm dev        # vite dev server on :5173, proxies /api → http://localhost:9997
pnpm build      # tsc --noEmit + vite build → dist/
```

## API consumed (same origin)

| Endpoint | Purpose |
| --- | --- |
| `GET /api/dashboard/state` | `{paused, paused_at, uptime_s, in_flight:[{id,model,path,stream,state,started_at,age_seconds,updated_at}], keepalive:{delay,interval}, events:[{ts,action}], temp_guard:{enabled,config_enabled,user_enabled,fail_open,max_temp,checked_at,last_error}, concurrency:{limit,active,queued,default_limit}}` (events newest first; `temp_guard` and `concurrency` are optional — the UI hides each strip when absent; `checked_at` is wall-clock epoch seconds, `null` before the first snapshot; when the guard is armed by config `temp_guard` also carries `stall_above`, `resume_below` and the `default_*` pair the UI offers to reset to; `concurrency.limit`/`default_limit` of `0` means unlimited, `active` is how many requests are at the LLM server, `queued` how many are waiting for a slot; `in_flight[].state` is one of `waiting`/`paused`/`stalled`/`queued`/`upstream`) |
| `POST /api/dashboard/pause` | hold all new requests (two-step confirm in the UI) |
| `POST /api/dashboard/resume` | release the hold |
| `POST /api/dashboard/tempguard` | body `{"enabled":bool}` — flips the guard's `user_enabled` switch; `{"ok":true,"enabled":…}`; 400 on malformed body |
| `POST /api/dashboard/tempguard/thresholds` | body `{"stall_above":n,"resume_below":n}` (either key alone works) or `{"reset":true}` — edits the live hysteresis pair in place; `{"ok":true,"stall_above":…,"resume_below":…}`; 400 with `{"error":…}` when the pair is invalid or the guard is disarmed by config |
| `POST /api/dashboard/concurrency` | body `{"limit":n}` (0 = unlimited, max 10000) or `{"reset":true}` — edits the live concurrent-request cap in place; `{"ok":true,"limit":…,"default_limit":…}`; raising the cap admits as many queued requests as the new capacity allows, lowering it never disturbs requests already running; 400 with `{"error":…}` when the value is not a whole number in range |
| `GET /api/dashboard/history?page=&size=` | `{total,page,size,pages,entries:[…]}` — server-side paginated request history from `stats.jsonl` |
| `GET /api/dashboard/healthz` | `{ok, uptime_s}` |

Every field is parsed defensively; if the backend is unreachable or drifts
from the contract, the UI degrades to a "proxy unreachable" banner instead
of throwing.
