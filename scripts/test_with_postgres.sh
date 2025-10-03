#!/usr/bin/env bash
set -euo pipefail

function usage() {
  cat <<'EOF'
Usage: scripts/test_with_postgres.sh [pytest args...]

Spin up the Postgres test database via docker compose, run migrations and
seed data using tests/scripts/bootstrap_test_db.py, execute pytest, then
tear everything down.

Environment:
  TEST_DATABASE_URL  Overrides the default postgres connection string
                     (default: postgresql://cms_user:cms_password@localhost:5432/cms_pricing)
  DOCKER_COMPOSE_BIN Command to invoke docker compose (default: docker compose)
  ALEMBIC_INI        Path to alembic.ini (default: alembic.ini)

Examples:
  scripts/test_with_postgres.sh tests/api/test_plans.py
  TEST_DATABASE_URL=postgresql://localhost:5555/test scripts/test_with_postgres.sh
EOF
}

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
  esac
fi

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$PROJECT_ROOT"

DOCKER_COMPOSE_BIN=${DOCKER_COMPOSE_BIN:-docker compose}
TEST_DATABASE_URL=${TEST_DATABASE_URL:-postgresql://cms_user:cms_password@localhost:5432/cms_pricing}
ALEMBIC_INI=${ALEMBIC_INI:-alembic.ini}
PYTEST_ARGS=(${@:-pytest})
PG_WAIT_TIMEOUT=${PG_WAIT_TIMEOUT:-120}
PG_WAIT_INTERVAL=${PG_WAIT_INTERVAL:-3}

function cleanup() {
  echo "\n[cleanup] Bringing down Postgres container" >&2
  $DOCKER_COMPOSE_BIN down db >/dev/null 2>&1 || true
}

trap cleanup EXIT

function wait_for_postgres() {
  echo "[wait] Checking Postgres readiness at $TEST_DATABASE_URL" >&2
  TEST_DATABASE_URL="$TEST_DATABASE_URL" PG_WAIT_TIMEOUT="$PG_WAIT_TIMEOUT" PG_WAIT_INTERVAL="$PG_WAIT_INTERVAL" python - <<'PY'
import os
import sys
import time
from urllib.parse import urlsplit

import psycopg2
from psycopg2 import OperationalError

database_url = os.environ["TEST_DATABASE_URL"]
timeout = int(os.environ["PG_WAIT_TIMEOUT"])
interval = float(os.environ["PG_WAIT_INTERVAL"])

deadline = time.time() + timeout
attempt = 0

while True:
    attempt += 1
    try:
        conn = psycopg2.connect(database_url)
        conn.close()
        print(f"[wait] Postgres ready after {attempt} attempt(s)")
        break
    except OperationalError as exc:
        if time.time() >= deadline:
            print(f"[wait] Gave up waiting for Postgres: {exc}", file=sys.stderr)
            sys.exit(1)
        time.sleep(interval)
PY
}

set -x
$DOCKER_COMPOSE_BIN up -d db
wait_for_postgres
python tests/scripts/bootstrap_test_db.py \
  --database-url "$TEST_DATABASE_URL" \
  --alembic-ini "$ALEMBIC_INI"
python -m pytest "${PYTEST_ARGS[@]}"
