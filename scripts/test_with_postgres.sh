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

if [[ $# -gt 0 && "$1" == "pytest" ]]; then
  shift
fi

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$PROJECT_ROOT"

DOCKER_COMPOSE_BIN=${DOCKER_COMPOSE_BIN:-docker compose}
TEST_DATABASE_URL=${TEST_DATABASE_URL:-postgresql://cms_user:cms_password@localhost:5432/cms_pricing}
ALEMBIC_INI=${ALEMBIC_INI:-alembic.ini}
PYTEST_ARGS=("$@")
PG_WAIT_TIMEOUT=${PG_WAIT_TIMEOUT:-120}
PG_WAIT_INTERVAL=${PG_WAIT_INTERVAL:-3}
PYTHON_BIN=${PYTHON_BIN:-python}
STARTED_COMPOSE_DB=0

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN=python3
fi

function cleanup() {
  if [[ "$STARTED_COMPOSE_DB" == "1" ]]; then
    echo "\n[cleanup] Bringing down Postgres container" >&2
    $DOCKER_COMPOSE_BIN down db >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

function wait_for_postgres() {
  echo "[wait] Checking Postgres readiness at $TEST_DATABASE_URL" >&2
  TEST_DATABASE_URL="$TEST_DATABASE_URL" PG_WAIT_TIMEOUT="$PG_WAIT_TIMEOUT" PG_WAIT_INTERVAL="$PG_WAIT_INTERVAL" "$PYTHON_BIN" - <<'PY'
import os
import sys
import time

import psycopg2
from psycopg2 import OperationalError
from sqlalchemy.engine import make_url

database_url = os.environ["TEST_DATABASE_URL"]
timeout = int(os.environ["PG_WAIT_TIMEOUT"])
interval = float(os.environ["PG_WAIT_INTERVAL"])

def candidate_urls(url):
    yield url
    parsed = make_url(url)
    if parsed.drivername.startswith("postgresql") and parsed.database != "postgres":
        yield parsed.set(database="postgres").render_as_string(hide_password=False)

deadline = time.time() + timeout
attempt = 0

while True:
    attempt += 1
    last_error = None
    for url in candidate_urls(database_url):
        try:
            conn = psycopg2.connect(url)
            conn.close()
            print(f"[wait] Postgres ready after {attempt} attempt(s)")
            sys.exit(0)
        except OperationalError as exc:
            last_error = exc
    if time.time() >= deadline:
        print(f"[wait] Gave up waiting for Postgres: {last_error}", file=sys.stderr)
        sys.exit(1)
    time.sleep(interval)
PY
}

function postgres_is_ready() {
  TEST_DATABASE_URL="$TEST_DATABASE_URL" "$PYTHON_BIN" - <<'PY'
import os
import sys

import psycopg2
from psycopg2 import OperationalError
from sqlalchemy.engine import make_url

def candidate_urls(url):
    yield url
    parsed = make_url(url)
    if parsed.drivername.startswith("postgresql") and parsed.database != "postgres":
        yield parsed.set(database="postgres").render_as_string(hide_password=False)

for url in candidate_urls(os.environ["TEST_DATABASE_URL"]):
    try:
        conn = psycopg2.connect(url)
        conn.close()
        sys.exit(0)
    except OperationalError:
        pass
sys.exit(1)
PY
}

set -x
if postgres_is_ready; then
  echo "[wait] Reusing reachable Postgres at $TEST_DATABASE_URL" >&2
else
  $DOCKER_COMPOSE_BIN up -d db
  STARTED_COMPOSE_DB=1
fi
wait_for_postgres
"$PYTHON_BIN" tests/scripts/bootstrap_test_db.py \
  --database-url "$TEST_DATABASE_URL" \
  --alembic-ini "$ALEMBIC_INI"
"$PYTHON_BIN" -m pytest "${PYTEST_ARGS[@]}"
