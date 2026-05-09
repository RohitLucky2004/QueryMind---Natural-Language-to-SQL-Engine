# querymind-schema-service/Dockerfile
# Multi-stage build — same image serves both FastAPI and Celery worker.
# CMD is overridden in docker-compose.yml for the Celery container.

# ── Stage 1: builder ──────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# System deps for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: runtime ──────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Non-root user for security
RUN useradd --create-home --shell /bin/sh appuser

WORKDIR /app

# Runtime system deps only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy shared package and install it
COPY --chown=appuser:appuser ../querymind-shared /app/querymind-shared
RUN pip install --no-cache-dir -e /app/querymind-shared

# Copy service source
COPY --chown=appuser:appuser . /app/

USER appuser

EXPOSE 8001

# Default command: run FastAPI app
# Overridden in docker-compose.yml for Celery worker containers
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--log-level", "info"]
