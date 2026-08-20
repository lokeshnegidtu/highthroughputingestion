# Multi-stage Dockerfile for AegisIngest Processing Worker
# Stage 1: Builder
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Stage 2: Minimal Runtime
FROM python:3.11-slim AS runtime

WORKDIR /app

# Create non-root user
RUN groupadd -g 10001 aegis && \
    useradd -u 10001 -g aegis -s /bin/bash -m aegisuser

COPY --from=builder /root/.local /home/aegisuser/.local
ENV PATH=/home/aegisuser/.local/bin:$PATH
ENV PYTHONPATH=/app

# Copy application source
COPY --chown=aegisuser:aegis src/ /app/worker/src/

# Ensure storage directory permissions
RUN mkdir -p /data/storage/blobs && \
    chown -R aegisuser:aegis /data/storage

USER aegisuser

EXPOSE 9100

CMD ["python", "-u", "worker/src/main.py"]
