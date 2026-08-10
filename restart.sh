# Full restart of the LLM proxy (OpenResty/Lua rewrite + SSE proxy on :9999).
# Removes all proxy containers, including the legacy njs-based 'llm-proxy'
# (an orphan of the same compose project), then recreates the OpenResty
# container from scratch (fresh config + fresh container).
#
# Old njs-based setup (kept for reference):
# docker compose down -v
# docker compose up -d --force-recreate

docker compose -f docker-compose.openresty.yaml down -v --remove-orphans
docker compose -f docker-compose.openresty.yaml up -d
