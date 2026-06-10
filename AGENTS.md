# Codex Instructions For `cms_api`

## Mission

This repo powers a CMS pricing API for ClearBill-style healthcare transparency.
Treat it as mission-critical healthcare reimbursement software. Incorrect MPFS,
OPPS, GPCI, modifier, locality, or effective-date behavior can create financial
liability.

Default review posture:

- Be explicit about pricing correctness risks.
- Prefer small, testable changes over broad refactors.
- Preserve user work and unrelated dirty files.
- Do not commit generated CMS data artifacts unless explicitly requested.
- Keep code PRs focused on code, tests, and directly relevant docs.

## Codex Project Setup

Create the Codex project with the repo root as the workspace:

```text
/Users/alexanderbea/Cursor/cms-api
```

Do not use a generated projectless Codex folder as the working directory for
repo changes. New Codex threads should start from this repo root so local paths,
Docker Compose commands, tests, and Git branches all resolve consistently.

## Domain Guardrails

Pricing math must avoid binary floats for money. Use Decimal or scaled integer
cents for reimbursement amounts, conversion factors, and beneficiary cost
sharing.

MPFS pricing must preserve the standard formula:

```text
(Work RVU * Work GPCI) + (PE RVU * PE GPCI) + (MP RVU * MP GPCI) = total RVU
total RVU * conversion factor = allowed amount
```

Required CMS edge cases:

- ZIP, carrier, and locality identifiers must remain strings. Preserve leading
  zeros and normalize `00` versus `01` intentionally.
- Modifier logic for `26`, `TC`, and global service pricing must be explicit.
- Missing RVU, GPCI, conversion factor, and rate values should fail clearly
  unless a CMS rule explicitly defines a zero value.
- CMS release and effective dates are pricing-critical. Snapshot registration
  and selection should use CMS effective dates, not ingestion run dates.
- Year and quarter selection must be explicit when validating pricing behavior.

## Branch And PR Discipline

Use a dedicated branch per workstream.

Code PRs should not include tracker state or generated tracker views. Exclude
these from ordinary implementation PRs:

- `state/work/**`
- `state/plans/accepted.yaml`
- `docs/workbench/CURRENT.md`
- `docs/workbench/ROADMAP.md`

If tracker/governance behavior changes, use a dedicated tracker PR. The rule is:

```text
Code PRs ship code. End-of-day tracker PRs reconcile tracker truth.
```

## Local Development

Prefer Docker Compose for first-pass local validation because it controls API,
Postgres, Redis, and worker dependencies.

```bash
docker compose -p cms-api-fix up -d --build db redis
docker compose -p cms-api-fix run --rm api python scripts/bootstrap_local_db.py --stamp-head
docker compose -p cms-api-fix up -d api worker
```

Useful smoke checks:

```bash
curl http://127.0.0.1:8000/healthz
curl http://127.0.0.1:8000/readyz
curl -H 'X-API-Key: dev-key-123' 'http://127.0.0.1:8000/geography/resolve?zip=94110'
```

Protected endpoints generally require:

```text
X-API-Key: dev-key-123
```

## RVU Loading

Repeatable local/dev command for live CMS RVU ingest with real DB writes:

```bash
docker compose -p cms-api-fix run --rm api \
  python scripts/load_latest_cms_rvu_local.py \
  --start-year 2026 \
  --end-year 2026 \
  --release latest \
  --output-dir data/ingestion/local/rvu \
  --report-json data/ingestion/local/reports/cms_rvu_local_load_latest.json
```

Expected validation coverage for this command:

- curated parquet exists for `pprrvu`, `gpci`, `oppscap`, `anescf`, and
  `localitycounty`
- `dataset_snapshots` rows use CMS effective dates
- DB tables have positive row counts for the loaded release
- pricing/snapshot lookup selects the expected RVU and GPCI releases by
  valuation date

## Tests

Run the narrowest relevant tests first, then broaden as risk increases.

Common targeted suites:

```bash
.venv/bin/pytest tests/scripts/test_load_latest_cms_rvu_local.py -q
.venv/bin/pytest tests/ingestors/test_rvu_loader_aliases.py -q
.venv/bin/pytest tests/tools/test_work_tracker.py -q
.venv/bin/pytest tests/api/test_health.py tests/api/test_plans.py -q
```

For live CMS validation, expect network and file availability to affect runtime.
Do not treat the full ingestor suite as acceptance for a narrow local boot or
RVU workflow change unless the change actually touches shared ingestion
behavior.

## Tracker Workflow

After PR #442 lands, use the repo-native tracker for status preservation.

During the day, local tracker edits are allowed for working context, but do not
stage them into ordinary code PRs.

At end of day, reconcile tracker truth in one tracker-only PR:

```bash
python scripts/governance/check-work-tracker.py
python scripts/governance/process_merge_queue.py --dry-run --json
python scripts/governance/build-work-tracker.py
git diff --check
python tools/work_tracker.py check-views
```

Generated views are not hand-edited:

- `docs/workbench/CURRENT.md`
- `docs/workbench/ROADMAP.md`

Resolve tracker YAML first, then regenerate views.

## Observability And Failure Handling

Do not swallow broad exceptions in startup, ingestion, pricing, or publishing
paths. If a CMS file changes shape, column drift should be visible as a
validation failure with enough context to debug the source file and release.

When adding ingestion or pricing behavior, preserve or add evidence paths:

- validation reports
- parser reject counts
- published artifact paths
- snapshot release IDs
- effective dates
- DB row counts

## Git Hygiene

Before committing:

```bash
git status --short
git diff --check
```

If the repo-wide markdown checkbox hook fails on unrelated legacy docs, do not
edit that legacy debt inside unrelated PRs. Document the reason if committing
with hooks bypassed.

Before pushing a PR branch:

```bash
git diff --name-only origin/main...HEAD
gh pr checks <PR_NUMBER>
```
