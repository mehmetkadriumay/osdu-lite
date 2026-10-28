FROM python:3.12-alpine
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir --only-binary=:all: --require-hashes -r /app/requirements.txt
COPY server.py /app/server.py
COPY reference_services.py /app/reference_services.py
COPY data /app/data
COPY files /app/files
EXPOSE 8089
ENV PORT=8089 OSDU_PARTITION=company-prod OSDU_TOKEN=demo-token REQUIRE_AUTH=true PRELOAD_FULL_SAMPLE=true INDEX_DELAY_SECONDS=0.7 PROJ_NETWORK=OFF
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=6 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8089/health', timeout=2)"
CMD ["python", "/app/server.py"]