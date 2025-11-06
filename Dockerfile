# syntax=docker/dockerfile:1.6

ARG PYTHON_IMAGE=python:3.11-slim-bookworm

# -----------------------------------------------------------------------------
# Builder stage: installs build tooling and production Python dependencies
# -----------------------------------------------------------------------------
FROM ${PYTHON_IMAGE} AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements separately to maximize layer caching
COPY requirements.txt requirements.prod.txt ./

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements.prod.txt

# -----------------------------------------------------------------------------
# Dev dependency stage: adds lint/test tooling on top of builder venv
# -----------------------------------------------------------------------------
FROM builder AS dev-deps

COPY requirements-dev.txt ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements-dev.txt

# -----------------------------------------------------------------------------
# Development image: keeps full source tree for hot reload
# -----------------------------------------------------------------------------
FROM dev-deps AS development

WORKDIR /app
COPY . .

CMD ["uvicorn", "cms_pricing.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# -----------------------------------------------------------------------------
# Production image: slim runtime with only required artifacts
# -----------------------------------------------------------------------------
FROM ${PYTHON_IMAGE} AS production

LABEL org.opencontainers.image.title="cms-pricing-api" \
      org.opencontainers.image.description="CMS Treatment Plan Pricing & Comparison API production image" \
      org.opencontainers.image.source="https://github.com/your-org/cms-pricing-api" \
      org.opencontainers.image.version="0.0.0"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

# Copy prebuilt virtualenv (Python deps)
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

# Copy only runtime application code and configs
COPY --chown=appuser:appuser cms_pricing/ ./cms_pricing/
COPY --chown=appuser:appuser alembic/ ./alembic/
COPY --chown=appuser:appuser alembic.ini .
COPY --chown=appuser:appuser pyproject.toml .
COPY --chown=appuser:appuser tests/scripts/ ./tests/scripts/

# Create data directories with proper permissions BEFORE switching to non-root
RUN mkdir -p data/observability data/metrics data/quarantine data/cache && \
    chown -R appuser:appuser data

# Switch to non-root user
USER appuser

# Health check (use PORT env var, default 8000)
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8000}/health || exit 1

# Run migrations/bootstrap and start server
# Use $PORT for Render compatibility (Render sets PORT=10000)
# Single worker to reduce memory footprint (512MB target for Starter tier)
CMD ["sh", "-c", "set -e; if [ \"$RUN_BOOTSTRAP_TEST_DB\" = \"true\" ] || [ \"$ENVIRONMENT\" = \"staging\" ]; then python tests/scripts/bootstrap_test_db.py --alembic-ini /app/alembic.ini; elif [ \"$RUN_MIGRATIONS\" = \"true\" ]; then alembic upgrade head; fi; uvicorn cms_pricing.main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1"]
