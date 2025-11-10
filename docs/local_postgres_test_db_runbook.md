# Local Postgres Test Database Runbook

This runbook describes how to provision, migrate, verify, and tear down a Postgres instance for running the CMS Pricing API test suite locally. The steps mirror the connection defaults shipped in `env.example` and used by pytest.

---

## 1. Prerequisites

- Docker Desktop running on your workstation.
- Python 3.11 virtual environment for this repository (see `docs/dev_setup.md`).
- `.env` file created from `env.example`, keeping:
  - `DATABASE_URL=postgresql://cms_user:cms_password@localhost:5432/cms_pricing`
  - `TEST_DATABASE_URL` pointing at the same connection string (or an alternate host/port if you change it below).

---

## 2. Launch Postgres (Docker)

### Option A — One-liner

```bash
docker run -d --name cms-postgres \
  -e POSTGRES_DB=cms_pricing \
  -e POSTGRES_USER=cms_user \
  -e POSTGRES_PASSWORD=cms_password \
  -p 5432:5432 \
  -v cms_pgdata:/var/lib/postgresql/data \
  postgres:15-alpine
```

- `cms_pgdata` is an optional named volume so migrations persist across restarts.
- Change the host port (`-p 5432:5432`) if you already have Postgres running locally.

### Option B — Scripted helper

```bash
./setup_docker_db.sh
```

The script creates the container, applies migrations, and loads sample data. Inspect the script before running if you need to adjust credentials or ports.

### Option C — Docker Compose

If you prefer to run the full stack, `docker-compose up -d` (documented in `HOW_TO_RUN_LOCALLY.md`) starts Postgres, Redis, and the API. You can still follow the migration and verification steps below.

---

## 3. Run Migrations

From an activated virtual environment:

```bash
alembic upgrade head
```

This applies all schema migrations to the running container. You may also use:

```bash
python scripts/bootstrap_test_db.py
```

which runs the same migration command and seeds the `dataset_snapshots` table with baseline entries.

---

## 4. Verify Connectivity

### Postgres health check

```bash
pg_isready -h localhost -p 5432 -U cms_user
```

### Basic query

```bash
psql postgresql://cms_user:cms_password@localhost:5432/cms_pricing -c '\dt'
```

You should see the list of tables created by Alembic.

---

## 5. Smoke Test Pytest

Run a small subset of tests to confirm the database is wired correctly:

```bash
pytest tests/services/test_dataset_snapshot_service.py -k select_snapshot
```

If this passes, the rest of the suite can access the database. When running the full suite remember that pytest reads `TEST_DATABASE_URL`; keep it pointed at the same instance or a dedicated clone.

---

## 6. Run Targeted Tests

```bash
# Service layer snapshot tests
pytest tests/services/test_dataset_snapshot_service.py

# Repair CLI tests (requires fallback directories)
pytest tests/ops/test_repair_snapshot_paths.py

# Entire suite
pytest
```

When running multiple test jobs in parallel, consider giving each job its own database (for example, different ports or database names) to avoid clobbering data.

---

## 7. Teardown

```bash
docker rm -f cms-postgres
# Optional: remove the named volume
docker volume rm cms_pgdata
```

If you used Docker Compose, run `docker-compose down` (add `-v` if you want to delete the attached volumes).

---

## 8. Troubleshooting

| Symptom | Potential Fix |
|---------|----------------|
| `Error: port is already allocated` | Stop other Postgres instances or change `-p 5432:5432` to another host port. |
| `psycopg2.errors.InvalidPassword` | Ensure `POSTGRES_USER`/`POSTGRES_PASSWORD` match the values in `.env`. |
| `relation "dataset_snapshots" does not exist` | Run `alembic upgrade head` again. |
| pytest reports `InFailedSqlTransaction` | Typically happens when the first DB statement failed. Check the stack trace for connection errors, re-run migrations, and ensure Postgres is reachable. |
| Need to reuse an existing dataset drop | Set `SNAPSHOT_SEARCH_ROOTS` (e.g., `/var/data/ingestion/production/curated`) before running repair/audit CLIs so fallback logic can find the mounted parquet files. |

---

## 9. Optional Enhancements

- Add Make targets (`make db_up`, `make db_down`) that wrap the Docker commands above.
- Extend `setup_docker_db.sh` with additional seed data or fixtures.
- Use Docker Compose profiles for integration tests vs. full API workloads.

---

## 10. References

- `env.example` — Source of default `DATABASE_URL` and `TEST_DATABASE_URL`.
- `setup_docker_db.sh` — Scripted Postgres bootstrap with migrations.
- `docs/dev_setup.md` — Full host environment bootstrap.
- `HOW_TO_RUN_LOCALLY.md` — Running the entire stack with Docker Compose.


