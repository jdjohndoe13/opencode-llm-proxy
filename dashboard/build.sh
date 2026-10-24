#!/bin/sh
# Build the dashboard in Docker and extract dist/ to the host.
# Run from the dashboard/ directory.
set -e
NAME=llm-proxy-dashboard
TMP=tmp-llm-dash
docker build -t "$NAME" .
rm -rf ./dist
docker rm -f "$TMP" 2>/dev/null || true
docker create --name "$TMP" "$NAME" >/dev/null
docker cp "$TMP":/app/dist ./dist
docker rm -f "$TMP" >/dev/null
echo
echo "OK: dashboard/dist generated."
