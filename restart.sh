# docker compose down -v
# docker compose up -d --force-recreate

docker compose -f docker-compose.openresty.yaml down -v
docker compose -f docker-compose.openresty.yaml up -d