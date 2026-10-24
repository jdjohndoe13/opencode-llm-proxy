# syntax=docker/dockerfile:1
# Build context is the repository root (see docker-compose.python.yaml):
# the dashboard frontend is built here in Docker (node + pnpm via
# corepack), so the host needs neither — only Docker.
FROM node:22-alpine AS frontend
WORKDIR /app

# pin pnpm via the "packageManager" field in package.json (corepack)
ENV COREPACK_ENABLE_DOWNLOAD_PROMPT=0
RUN corepack enable

# deps first — only re-installed when the lockfile changes
COPY dashboard/package.json dashboard/pnpm-lock.yaml ./
RUN --mount=type=cache,id=pnpm,target=/root/.local/share/pnpm/store \
    pnpm install --frozen-lockfile

COPY dashboard/tsconfig.json dashboard/vite.config.ts dashboard/index.html ./
COPY dashboard/src ./src
RUN pnpm build

# ---------------------------------------------------------------------------
FROM python:3.12-slim

WORKDIR /app

COPY python/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY python/llm_proxy ./llm_proxy

# Dashboard UI from the frontend stage above (the checked-in placeholder in
# python/llm_proxy/static is only used for local runs without the build).
COPY --from=frontend /app/dist ./llm_proxy/static

# Drop privileges; LOG_DIR target lives in the bind-mounted logs volume.
RUN useradd -r -u 10001 app \
    && mkdir -p /var/log/nginx/completions \
    && chown -R app:app /app /var/log/nginx/completions
USER app

EXPOSE 9999

# slim has no curl/wget; check the local /health endpoint (never touches
# the upstream, so a slow backend cannot flap the container).
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD ["python","-c","import urllib.request,sys;sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:9999/health',timeout=2).status==200 else 1)"]

CMD ["python","-m","llm_proxy"]
