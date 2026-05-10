# ── Stage 1: builder ──────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

# Copy and install shared package first (layer cache)
COPY ../querymind-shared /build/querymind-shared
RUN pip install --no-cache-dir /build/querymind-shared

# Copy and install service dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: runtime ──────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY . .

# Ensure non-root ownership
RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8002

# Default: run FastAPI. Override CMD to run Celery worker.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
