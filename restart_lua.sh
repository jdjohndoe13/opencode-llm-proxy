#!/bin/sh
# Full restart of the LLM proxy: removes all containers of this compose
# project (including orphans) and recreates the OpenResty container from
# scratch (fresh config + fresh container). Values are read from .env.
docker compose down -v --remove-orphans
docker compose up -d --force-recreate
