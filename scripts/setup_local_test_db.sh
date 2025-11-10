#!/usr/bin/env bash
#
# setup_local_test_db.sh
# ---------------------------------------------------------------------
# Provision a local Postgres instance (via Docker) that matches the
# connection defaults used by the CMS Pricing API and pytest.  The script
# can optionally run Alembic migrations and a smoke-test subset of the
# suite once the database is available.
#
# Usage:
#   ./scripts/setup_local_test_db.sh [--no-migrations] [--run-smoke-tests]
#       [--port 5432] [--container cms-postgres] [--volume cms_pgdata]
#       [--image postgres:15-alpine]
#
# Examples:
#   ./scripts/setup_local_test_db.sh
#   ./scripts/setup_local_test_db.sh --port 55432 --container cms-test-db
#   ./scripts/setup_local_test_db.sh --run-smoke-tests
#
# Requires:
#   - Docker CLI
#   - alembic (available in your virtualenv) if migrations are enabled
#   - pytest (for optional smoke tests)
#
set -euo pipefail

CONTAINER_NAME="cms-postgres"
DB_NAME="cms_pricing"
DB_USER="cms_user"
DB_PASSWORD="cms_password"
HOST_PORT="5432"
IMAGE="postgres:15-alpine"
VOLUME_NAME="cms_pgdata"
RUN_MIGRATIONS=true
RUN_SMOKE=false
SMOKE_TARGET="tests/services/test_dataset_snapshot_service.py -k select_snapshot"
WAIT_TIMEOUT=120
RETRY_INTERVAL=5

usage() {
  grep '^#' "$0" | cut -c 4-
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --no-migrations)
      RUN_MIGRATIONS=false
      shift
      ;;
    --run-smoke-tests)
      RUN_SMOKE=true
      shift
      ;;
    --port)
      HOST_PORT="$2"
      shift 2
      ;;
    --container)
      CONTAINER_NAME="$2"
      shift 2
      ;;
    --volume)
      VOLUME_NAME="$2"
      shift 2
      ;;
    --image)
      IMAGE="$2"
      shift 2
      ;;
    --smoke-target)
      SMOKE_TARGET="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

log() {
  printf '\033[1;34m[setup-db]\033[0m %s\n' "$*"
}

warn() {
  printf '\033[1;33m[setup-db]\033[0m %s\n' "$*" >&2
}

error() {
  printf '\033[1;31m[setup-db]\033[0m %s\n' "$*" >&2
  exit 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    error "Missing required command: $1"
  fi
}

require_command docker

if $RUN_MIGRATIONS; then
  require_command alembic
fi

if $RUN_SMOKE; then
  require_command pytest
fi

DATABASE_URL="postgresql://${DB_USER}:${DB_PASSWORD}@localhost:${HOST_PORT}/${DB_NAME}"

ensure_container() {
  if docker ps --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
    log "Container '${CONTAINER_NAME}' already running"
    return
  fi

  if docker ps -a --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
    log "Starting existing container '${CONTAINER_NAME}'"
    docker start "$CONTAINER_NAME" >/dev/null
    return
  fi

  log "Launching Postgres container '${CONTAINER_NAME}' on port ${HOST_PORT}"
  docker run -d \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_DB="$DB_NAME" \
    -e POSTGRES_USER="$DB_USER" \
    -e POSTGRES_PASSWORD="$DB_PASSWORD" \
    -p "${HOST_PORT}:5432" \
    -v "${VOLUME_NAME}:/var/lib/postgresql/data" \
    "$IMAGE" >/dev/null
}

wait_for_ready() {
  local waited=0
  log "Waiting for Postgres to accept connections (timeout ${WAIT_TIMEOUT}s)..."

  until docker exec "$CONTAINER_NAME" pg_isready -U "$DB_USER" -d "$DB_NAME" >/dev/null 2>&1; do
    sleep "$RETRY_INTERVAL"
    waited=$((waited + RETRY_INTERVAL))
    if (( waited >= WAIT_TIMEOUT )); then
      error "Postgres did not become ready within ${WAIT_TIMEOUT} seconds."
    fi
  done
  log "Postgres ready after ${waited}s."
}

run_migrations() {
  if ! $RUN_MIGRATIONS; then
    log "Skipping Alembic migrations (per flag)"
    return
  fi

  if [[ -z "${VIRTUAL_ENV:-}" ]]; then
    warn "You are not in a virtualenv; alembic will run with the system interpreter."
  fi

  log "Running Alembic migrations..."
  DATABASE_URL="$DATABASE_URL" alembic upgrade head
  log "Migrations completed."
}

smoke_test() {
  if ! $RUN_SMOKE; then
    return
  fi
  log "Running pytest smoke test (${SMOKE_TARGET})..."
  DATABASE_URL="$DATABASE_URL" TEST_DATABASE_URL="$DATABASE_URL" pytest $SMOKE_TARGET
  log "Smoke tests passed."
}

print_summary() {
  cat <<EOF

----------------------------------------------------------------------
Local Postgres test database is ready.

Connection string:
  ${DATABASE_URL}

Environment exports (add to your shell profile if desired):
  export DATABASE_URL="${DATABASE_URL}"
  export TEST_DATABASE_URL="${DATABASE_URL}"

Container management:
  docker ps --filter "name=${CONTAINER_NAME}"
  docker logs -f ${CONTAINER_NAME}
  docker stop ${CONTAINER_NAME}
  docker rm ${CONTAINER_NAME}
  docker volume rm ${VOLUME_NAME}   # optional, removes persisted data

To run the full test suite:
  TEST_DATABASE_URL="${DATABASE_URL}" pytest
----------------------------------------------------------------------
EOF
}

main() {
  ensure_container
  wait_for_ready
  export DATABASE_URL
  export TEST_DATABASE_URL="$DATABASE_URL"
  run_migrations
  smoke_test
  print_summary
}

main "$@"

