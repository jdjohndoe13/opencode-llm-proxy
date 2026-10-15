FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY llm_proxy ./llm_proxy

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
