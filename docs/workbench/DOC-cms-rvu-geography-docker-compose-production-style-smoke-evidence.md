# CMS RVU/Geography Docker Compose Production-Style Smoke Evidence

**Status:** Passed
**Updated:** 2026-06-11
**Scope:** Docker Compose smoke against isolated compose Postgres database
**Production mutation:** None

## Purpose

Record evidence that the RVU/geography production-style sequence works inside
the Docker Compose API environment against a compose Postgres database, not only
from the host virtualenv or a shared host-local database.

## Environment

- Compose project: `cms-api-fix`
- Database container: `cms-api-fix-db-1`
- Smoke database: `cms_pricing_docker_smoke_20260611_01`
- API image/container execution path: `docker compose -p cms-api-fix run --rm api`
- Consolidated report:
  `data/ingestion/local/reports/cms_pricing_docker_smoke_20260611_01.json`

The default compose project could not bind host port `5432` because the
documented `cms-api-fix` stack was already running and owned that port. The
smoke therefore reused the documented running stack and created an isolated
database inside its Postgres service.

## Commands

```bash
docker compose -p cms-api-fix exec -T db \
  createdb -U cms_user cms_pricing_docker_smoke_20260611_01
```

```bash
docker compose -p cms-api-fix run --rm \
  -e DATABASE_URL=postgresql://cms_user:cms_password@db:5432/cms_pricing_docker_smoke_20260611_01 \
  -e TEST_DATABASE_URL=postgresql://cms_user:cms_password@db:5432/cms_pricing_docker_smoke_20260611_01 \
  api python scripts/bootstrap_local_db.py \
  --database-url postgresql://cms_user:cms_password@db:5432/cms_pricing_docker_smoke_20260611_01 \
  --stamp-head
```

Result: schema bootstrap passed and Alembic was stamped to
`2f729579351f`.

```bash
docker compose -p cms-api-fix run --rm \
  -e DATABASE_URL=postgresql://cms_user:cms_password@db:5432/cms_pricing_docker_smoke_20260611_01 \
  -e TEST_DATABASE_URL=postgresql://cms_user:cms_password@db:5432/cms_pricing_docker_smoke_20260611_01 \
  api python scripts/run_cms_pricing_local_smoke.py \
  --database-url postgresql://cms_user:cms_password@db:5432/cms_pricing_docker_smoke_20260611_01 \
  --report-json data/ingestion/local/reports/cms_pricing_docker_smoke_20260611_01.json
```

Result: `status=ok`.

## Evidence Summary

Geography readiness:

- Source URL:
  `https://www.cms.gov/files/zip/zip-code-carrier-locality-file-revised-08/14/2025.zip`
- Release ID: `zip_locality_2025_Q4`
- Dataset digest:
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`
- Effective window: `2025-10-01` through open-ended latest mode
- Rows inserted: `1,118,970`
- ZIP5 rows: `42,956`
- ZIP9 rows: `1,076,014`
- Rejected rows: `0`
- Duplicate source keys: `0`
- Locality `00` rows: `39,476`
- Probe: `94110 -> CA/05/01112`
- Production readiness gates: pass, no failed gates

RVU load:

- Selected release: `rvu_2026_C`
- Selected URL: `https://www.cms.gov/files/zip/rvu26c.zip`
- Curated parquet rows:
  - `pprrvu`: `19,358`
  - `gpci`: `109`
  - `oppscap`: `15,260`
  - `anescf`: `109`
  - `localitycounty`: `117`
- DB rows:
  - `pprrvu`: `19,358`
  - `gpci`: `109`
  - `oppscap`: `15,260`
  - `anescf`: `109`
  - `localitycounty`: `57`
- Dataset snapshot effective dates: `2026-07-01`
- Snapshot selection at valuation date `2026-07-01`:
  - `rvu_items -> rvu_2026_C`
  - `gpci_indices -> gpci_2026_C`

Post-load API proof checks:

- `94110` resolved to `CA`, locality `05`, carrier `01112`
- `94110` MPFS proof returned `allowed_cents=11758`
- `66012` resolved to `EK`, locality `00`, carrier `05202`
- `66012` MPFS proof returned `allowed_cents=8902`
- Both proof checks selected `release_id=rvu_2026_C`
- Both proof checks used `proof_path=production_style_local_smoke`
- Seed-helper proof path was not used

## Stop Conditions Checked

- No production database URL was used.
- No seed helper was invoked.
- Locality `00` was preserved.
- Source rejects and duplicate source keys were zero.
- RVU/GPCI/CF trace refs were present in pricing proof output.
- Compose smoke did not rely on host-only Python state.

## Follow-Up

The next required artifact is the Render production execution runbook. Any
Render production mutation remains blocked until an operator explicitly approves
the runbook and target environment.
