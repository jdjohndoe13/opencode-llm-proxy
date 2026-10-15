#!/bin/sh
# Full restart of the LLM proxy (Python/aiohttp version): removes all
# containers of this compose project (including orphans) and recreates the
# Python container from scratch (fresh image + fresh container). Values are
# read from .env. See restart_lua.sh for the legacy OpenResty version.
docker compose -f docker-compose.python.yaml down -v --remove-orphans
docker compose -f docker-compose.python.yaml up -d --build --force-recreate
