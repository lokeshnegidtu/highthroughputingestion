# Multi-stage Dockerfile for AegisIngest Ingestion API
# Stage 1: Build & Dependencies
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Stage 2: Final Minimal Runtime
FROM python:3.11-slim AS runtime

WORKDIR /app

# Create non-root system user
RUN groupadd -g 10001 aegis && \
    useradd -u 10001 -g aegis -s /bin/bash -m aegisuser

# Copy installed wheels from builder
COPY --from=builder /root/.local /home/aegisuser/.local
ENV PATH=/home/aegisuser/.local/bin:$PATH
ENV PYTHONPATH=/app

# Copy application source
COPY --chown=aegisuser:aegis src/ /app/api/src/

# Ensure storage directories exist with correct permissions
RUN mkdir -p /data/storage/blobs /data/storage/tmp /app/data/storage/blobs /app/data/storage/tmp && \
    chown -R aegisuser:aegis /data/storage /app/data

ENV STORAGE_DIR=/data/storage

USER aegisuser

EXPOSE 8000

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')" || exit 1

CMD ["uvicorn", "api.src.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--loop", "uvloop", "--no-access-log"]
