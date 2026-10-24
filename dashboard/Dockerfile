# syntax=docker/dockerfile:1
# ============================================================================
# llm-proxy dashboard build — no node/pnpm needed on the host, Docker only.
#
#   docker build -t llm-proxy-dashboard .
#   docker create --name tmp llm-proxy-dashboard
#   docker cp tmp:/app/dist ./dist
#   docker rm tmp
#
# (or simply: build.cmd / build.sh)
#
# The release stage is `FROM scratch` and carries the Vite output at the
# filesystem root, so `docker build --output type=local,dest=dist .` also
# drops index.html + assets/ straight into ./dist.
# ============================================================================

FROM node:22-alpine AS build
WORKDIR /app

# pin pnpm via the "packageManager" field in package.json (corepack)
RUN corepack enable

# deps first — only re-installed when the lockfile changes
COPY package.json pnpm-lock.yaml ./
RUN --mount=type=cache,id=pnpm,target=/root/.local/share/pnpm/store \
    pnpm install --frozen-lockfile

COPY tsconfig.json vite.config.ts index.html ./
COPY src ./src
RUN pnpm build

# ---------------------------------------------------------------------------
# Release stage: dist at BOTH the root (for --output type=local) and
# /app/dist (for the docker cp flow).
# ---------------------------------------------------------------------------
FROM scratch AS release
COPY --from=build /app/dist /
COPY --from=build /app/dist /app/dist
# docker create refuses images without a command; the release stage is a
# data carrier only — it is never meant to run.
ENTRYPOINT ["/bin/true"]
