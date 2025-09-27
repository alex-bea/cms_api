# Docker build and run instructions for CMS Pricing API

This document explains how to build and run the `cms-pricing-api` docker image locally.

Prerequisites
- Docker installed and running
- A Postgres database available (local or remote)
- Optional: Redis instance for caching

Important environment variables
- DATABASE_URL (required) e.g. postgres://user:pass@host:5432/dbname
- REDIS_URL (optional) default redis://localhost:6379/0
- API_KEYS (optional) comma-separated API keys for access
- DEBUG (optional) if true, enables FastAPI docs and reload

Build image

```bash
cd /path/to/repo
docker build -t cms-pricing-api:local .
```

Run container (development, mounted code, reload)

```bash
docker run --rm -it \
  -p 8000:8000 \
  -e DATABASE_URL="postgres://user:pass@host:5432/db" \
  -e REDIS_URL="redis://host:6379/0" \
  -e DEBUG=true \
  -v "$PWD":/app \
  cms-pricing-api:local \
  uvicorn cms_pricing.main:app --host 0.0.0.0 --port 8000 --reload
```

Run container (production)

```bash
docker run -d --name cms-pricing \
  -p 8000:8000 \
  -e DATABASE_URL="postgres://user:pass@host:5432/db" \
  -e REDIS_URL="redis://host:6379/0" \
  cms-pricing-api:local
```

Health checks
- The Dockerfile sets a HEALTHCHECK that pings `/healthz` on port 8000. Ensure the service is healthy by running:

```bash
docker inspect --format='{{json .State.Health}}' cms-pricing | jq
```

Notes
- The image uses `alembic upgrade head` at container start; ensure the database is reachable when starting.
- For production deployments, use a proper secret management solution for DB and AWS credentials.
