# Multi-stage Dockerfile for AegisIngest Real-Time Dashboard
# Stage 1: Build
FROM python:3.11-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Stage 2: Runtime
FROM python:3.11-slim AS runtime

WORKDIR /app

# Create non-root user
RUN groupadd -g 10001 aegis && \
    useradd -u 10001 -g aegis -s /bin/bash -m aegisuser

COPY --from=builder /root/.local /home/aegisuser/.local
ENV PATH=/home/aegisuser/.local/bin:$PATH
ENV PYTHONPATH=/app

COPY --chown=aegisuser:aegis src/ /app/dashboard/src/

USER aegisuser

EXPOSE 8080

CMD ["uvicorn", "dashboard.src.app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
