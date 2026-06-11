# Render RVU/Geography Production Execution Runbook

**Status:** Draft for operator approval
**Updated:** 2026-06-11
**Scope:** Render production execution plan for public CMS ZIP-locality,
RVU, GPCI, conversion-factor, and post-load API smoke
**Production mutation:** Not approved by this document

## Purpose

Define the Render-specific execution sequence for loading the proven public CMS
RVU/geography data path into the Render production database. This runbook turns
the completed local and Docker evidence into a controlled production change, but
does not itself authorize the change.

## Target

- Render web service: `cms-pricing-api`
- Render database: `cms-pricing-db`
- Database name: `cms_pricing`
- Database user: `cms_user`
- Region: Oregon
- Runtime image: `ghcr.io/alex-bea/cms_api:<approved-sha-or-digest>`

Confirm these names in Render before running any command. Stop if the service,
database, image, branch, or environment differs from this target.

## Required Evidence Before Approval

- Local production-style evidence:
  `docs/workbench/DOC-cms-rvu-geography-local-production-preflight-evidence.md`
- Docker Compose evidence:
  `docs/workbench/DOC-cms-rvu-geography-docker-compose-production-style-smoke-evidence.md`
- Preflight runbook:
  `docs/workbench/DOC-cms-pricing-production-preflight-runbook.md`
- ZIP-locality policy:
  `docs/workbench/DOC-cms-zip-locality-source-discovery-effective-date-policy.md`

Current approved evidence baseline:

- ZIP-locality source URL:
  `https://www.cms.gov/files/zip/zip-code-carrier-locality-file-revised-08/14/2025.zip`
- ZIP-locality release ID: `zip_locality_2025_Q4`
- ZIP-locality digest:
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`
- ZIP-locality mode requiring approval for production:
  explicit latest-active/open-ended coverage for valuation date `2026-07-01`
- RVU release: `rvu_2026_C`
- RVU source URL: `https://www.cms.gov/files/zip/rvu26c.zip`
- Expected proof ZIPs:
  - `94110 -> CA/05/01112`, positive MPFS price through `rvu_2026_C`
  - `66012 -> EK/00/05202`, positive MPFS price through `rvu_2026_C`

## Approval Gate

An operator must approve all of the following before production mutation:

- exact Render service and database target;
- approved image SHA or digest;
- database backup or restore point;
- latest-active/open-ended geography behavior for production, or a newer strict
  source package that covers the valuation date;
- source URL, digest, release ID, row-count expectations, and zero-reject
  expectations;
- RVU release `rvu_2026_C` and snapshot effective date `2026-07-01`;
- scoped replace semantics for geography and RVU load;
- rollback path;
- final live API smoke probes and API key owner.

Without that approval, stop here.

## Execution Sequence

### 1. Confirm Deployment Artifact

Confirm the image to deploy is pinned by SHA or digest and matches the branch
that contains the current ingestion code, runbooks, and smoke scripts.

```bash
gh pr checks <approved-pr-number>
```

```bash
gh run list --limit 10
```

Stop if checks are failing, unknown, or tied to a different branch.

### 2. Deploy Or Confirm Render Image

Deploy through the existing Render workflow or Render Deploy API described in
`prds/RUN-render-deployment-prd-v1.0.md`. After deploy, confirm the running
service image matches the approved SHA or digest.

Stop if Render is running `latest` without a verified resolved image.

### 3. Confirm Schema State

Run migrations using the established Render one-off job or migration workflow.

```bash
alembic current
```

```bash
alembic upgrade head
```

```bash
alembic current
```

Stop if migration state is not head, if schema drift appears, or if the job
requires `Base.metadata.create_all()` against production.

### 4. Create Backup Or Restore Point

Before any data load, create or verify a Render database backup/restore point.
Record:

- backup identifier;
- timestamp;
- database name;
- operator;
- restore instructions.

Stop if the backup cannot be confirmed.

### 5. Dry-Run Geography Readiness On Render

Use a Render shell or one-off job with `DATABASE_URL` set by Render secrets.
This step must be read-only.

```bash
python scripts/load_cms_geography_local.py \
  --dry-run \
  --open-ended-latest \
  --require-valuation-date-coverage \
  --production-readiness-gates \
  --report-json /var/data/ingestion/production/reports/cms-geography-render-open-ended-readiness.json
```

Expected result:

- `status=ok`
- digest
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`
- `1,118,970` rows
- `0` rejects
- `0` duplicate source keys
- locality `00` preserved
- `94110 -> CA/05/01112`

Stop if the report differs from the approved evidence baseline.

### 6. Load Geography To Render

Run only after approval and backup confirmation. This is a production write.

```bash
python scripts/load_cms_geography_local.py \
  --allow-remote \
  --replace-existing \
  --open-ended-latest \
  --require-valuation-date-coverage \
  --production-readiness-gates \
  --report-json /var/data/ingestion/production/reports/cms-geography-render-load.json
```

Expected result:

- source package and digest match the dry run;
- runtime `geography` rows are loaded for `ZIP_LOCALITY`;
- snapshot `zip_locality_2025_Q4` is registered;
- readiness gates pass.

Stop if the command would delete unrelated geography rows or if scoped replace
semantics are not clear from the report.

### 7. Load RVU/GPCI/CF To Render

Use the proven latest CMS RVU loader, not the older
`scripts/load_rvu_to_production.py` helper. The older helper defaults to legacy
release behavior and is not the evidence-backed path for `rvu_2026_C`.

```bash
python scripts/load_latest_cms_rvu_local.py \
  --allow-remote \
  --start-year 2026 \
  --end-year 2026 \
  --release latest \
  --output-dir /var/data/ingestion/production/rvu \
  --report-json /var/data/ingestion/production/reports/cms-rvu-render-load-latest.json
```

Expected result:

- selected release `rvu_2026_C`;
- `pprrvu`, `gpci`, `oppscap`, `anescf`, and `localitycounty` curated parquet
  artifacts exist;
- DB row counts are positive and match the approved baseline unless reviewed;
- dataset snapshots use effective date `2026-07-01`;
- snapshot selection at valuation date `2026-07-01` selects
  `rvu_items -> rvu_2026_C` and `gpci_indices -> gpci_2026_C`.

Stop if the selected release is not `rvu_2026_C`.

### 8. Run Snapshot Audit And Preflight

Run the standard snapshot post-deploy evidence script if the production runtime
has the expected artifact directory.

```bash
bash scripts/render_snapshot_postdeploy.sh \
  /var/data/ingestion/production/reports/render-snapshot-evidence-$(date -u +%Y%m%dT%H%M%SZ)
```

Stop if snapshot paths cannot be resolved or if repair would point snapshots to
unapproved artifacts.

### 9. Run Render DB Proof Smoke

Run the smoke script inside Render against the production `DATABASE_URL`.

```bash
python scripts/post_rvu_load_api_smoke.py \
  --allow-remote \
  --valuation-date 2026-07-01 \
  --proof-path production_style_local_smoke
```

```bash
python scripts/post_rvu_load_api_smoke.py \
  --allow-remote \
  --zip 66012 \
  --valuation-date 2026-07-01 \
  --expected-state EK \
  --expected-locality 00 \
  --expected-carrier 05202 \
  --proof-path production_style_local_smoke
```

Expected result:

- both commands return `status=ok`;
- `94110 -> CA/05/01112`;
- `66012 -> EK/00/05202`;
- both prices are positive;
- both select `release_id=rvu_2026_C`;
- trace refs include RVU, GPCI, and conversion-factor provenance.

### 10. Run Live API Smoke

Use the production API key through the live service URL.

```bash
curl https://<render-service-host>/healthz
```

```bash
curl https://<render-service-host>/readyz
```

```bash
curl -H 'X-API-Key: <production-api-key>' \
  'https://<render-service-host>/geography/resolve?zip=94110'
```

Then run the production pricing endpoint or approved client query for HCPCS
`99213`, ZIP `94110`, and valuation date `2026-07-01`. Record response status,
release ID, allowed amount, and trace refs.

Stop if the live API does not agree with the database smoke.

## Rollback

Use the backup or restore point created before the load as the primary rollback
path. If rollback must be scoped instead of full database restore, restore the
previous approved geography and RVU/GPCI dataset snapshots and rows only after
reviewing the generated load reports.

Rollback triggers:

- wrong Render database target;
- source digest mismatch;
- nonzero source rejects or duplicate source keys;
- locality `00` loss;
- unexpected RVU release;
- missing GPCI or conversion-factor provenance;
- failed live smoke;
- unacceptable latency or API readiness failure after load.

## Stop Conditions

Stop immediately if:

- operator approval is absent;
- Render target is not `cms-pricing-api` and `cms-pricing-db`;
- backup or restore point is absent;
- source digest, URL, release ID, or row counts differ without review;
- latest-active/open-ended geography behavior is not explicitly approved;
- any command would use private data, secrets in logs, or a production URL
  outside Render;
- load evidence depends on `scripts/seed_post_rvu_load_local.py`;
- `scripts/load_rvu_to_production.py` is the only RVU load path available;
- final smoke cannot prove `94110`, `66012`, `rvu_2026_C`, `gpci_2026_C`, and
  conversion-factor provenance.

## Approval Record

Approval status: pending.

Use `docs/workbench/DOC-render-rvu-geography-production-approval-gate.md` to
record the operator decision before adding any production execution task.
