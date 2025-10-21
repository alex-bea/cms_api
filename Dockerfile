# Multi-stage build for CMS Pricing API
FROM python:3.11-slim AS base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Validate OpenAPI contract (skip if tests don't exist)
# RUN python -m pytest tests/test_openapi_consistency.py tests/test_error_contract.py

# Development stage
FROM base AS development
CMD ["uvicorn", "cms_pricing.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# Production stage
FROM base AS production

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Create data directories with proper permissions BEFORE switching to non-root
RUN mkdir -p data/observability data/metrics data/quarantine data/cache && \
    chown -R appuser:appuser data

# Switch to non-root user
USER appuser

# Health check (use PORT env var, default 8000)
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8000}/health || exit 1

# Run migrations and start server
# Use $PORT for Render compatibility (Render sets PORT=10000)
# Single worker to reduce memory footprint (512MB target for Starter tier)
CMD ["sh", "-c", "if [ \"$RUN_MIGRATIONS\" = \"true\" ]; then alembic upgrade head; fi && uvicorn cms_pricing.main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1"]