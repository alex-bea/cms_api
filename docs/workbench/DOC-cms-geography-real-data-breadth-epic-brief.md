# CMS Geography Real Data Breadth

**Status:** Active
**Updated:** 2026-06-11
**Tracker link:** `state/work/epics/cms-geography-real-data-breadth.yaml`

## Related Governance

- `prds/PRD-geography-locality-mapping-prd-v1.0.md`
- `prds/REF-geography-source-map-prd-v1.0.md`
- `prds/REF-cms-pricing-source-map-prd-v1.0.md`
- `prds/REF-nearest-zip-resolver-prd-v1.0.md`
- `prds/SRC-locality.md`
- `prds/SRC-gpci.md`
- `prds/PRD-rvu-gpci-prd-v0.1.md`
- `prds/STD-data-architecture-prd-v1.0.md`
- `prds/STD-data-architecture-impl-v1.0.md`
- `docs/workbench/DOC-cms-rvu-ingestion-epic-brief.md`
- `docs/workbench/DOC-cms-rvu-local-db-load-status.md`

## Goal

Replace single-ZIP geography proof with repeatable real-source geography
breadth so MPFS pricing can resolve ZIPs from public CMS geography data, join
them to loaded GPCI/RVU locality keys, and pass post-load smoke checks without
one-off database mutation.

## Current State

The RVU pricing path now uses loaded RVU/GPCI snapshots and can price ZIP
`94110` after `scripts/seed_post_rvu_load_local.py` inserts one public
ZIP-locality row and registers missing snapshot rows. That proves the pricing
and locality join path, but it does not prove real geography breadth.

The runtime resolver reads the `geography` table. The CMS ZIP Code to Carrier
Locality / ZIP9 source is the authoritative source for ZIP-to-locality breadth.
The RVU `localitycounty` file is useful for MAC/locality/county QA, but it is
not a substitute for loading ZIP-to-locality rows into `geography`.

The repository already contains geography contracts, source docs, resolver
tests, ZIP-locality ingester stubs, ZIP9 ingester code, and nearest-ZIP test
coverage. The known gap is that the current production ZIP-locality ingester
does not yet parse and publish the real CMS source into the runtime
`geography` table with effective dates, digests, and coverage gates.

## Source Audit Result

The harness selected `audit-geography-real-data-sources-and-gaps` as the active
task. That audit is complete.

Current public CMS pages establish the surrounding pricing context:

- the Physician Fee Schedule page is current for CY 2026 and links to PFS
  Carrier Specific Files, Medicare PFS Locality Configuration, Medicare PFS
  Locality Key, and PFS Relative Value Files;
- the PFS Relative Value Files page includes `RVU26C`, described by CMS as the
  July 2026 Physician Fee Schedule release;
- the Medicare PFS Locality Configuration page documents the current locality
  structure and says the CY 2024 California changes leave 29 California
  localities and 109 total PFS localities;
- the Carrier Specific Files page has CY 2026 All States carrier files, but
  those are fee files, not the ZIP-to-locality source required by the runtime
  resolver.

The current repository source map and local artifacts identify the concrete CMS
ZIP-locality package as:

- public CMS artifact:
  `https://www.cms.gov/files/zip/zip-code-carrier-locality-file-revised-08/14/2025.zip`;
- local copy:
  `data/cms_raw/zip-code-carrier-locality-file-revised-08-14-2025.zip`;
- extracted files:
  - `ZIP5_OCT2025.txt`
  - `ZIP5_OCT2025.xlsx`
  - `ZIP5lyout.txt`
  - `ZIP9_OCT2025.txt`
  - `ZIP9lyout.txt`

Local source facts from the extracted package:

| File | Rows | Relevant fields | Notes |
|---|---:|---|---|
| `ZIP5_OCT2025.txt` | 42,956 | state, ZIP5, carrier, pricing locality, rural indicator, plus-four flag, Part B payment indicator, year/quarter | `94110` appears as `CA`, carrier `01112`, locality `05`, year/quarter `20254`. |
| `ZIP9_OCT2025.txt` | 1,076,014 | state, ZIP5, carrier, pricing locality, rural indicator, plus-four flag, plus4, Part B payment indicator, year/quarter | All `(zip5, plus4)` keys were unique in the local audit. |

Domain checks from the audit:

- `ZIP5_OCT2025.txt` has 42,956 unique ZIP5 keys and no duplicate ZIP5 keys.
- `ZIP9_OCT2025.txt` has 1,076,014 unique `(zip5, plus4)` keys and no duplicate
  `(zip5, plus4)` keys.
- ZIP5 locality `00` appears 13,990 times, so `00` must be preserved as a real
  CMS source value and never normalized to benchmark/default locality `01`.
- ZIP `94110` can be satisfied from real ZIP5 source data with CA locality `05`;
  it should not require `scripts/seed_post_rvu_load_local.py` after a real
  geography load.

Implementation gap matrix:

| Area | Current state | Gap |
|---|---|---|
| Runtime resolver | `GeographyService` reads the `geography` table. | Real CMS ZIP-locality ingesters do not currently publish into `geography`. |
| Runtime table | `geography` supports `zip5`, `plus4`, `has_plus4`, `state`, `locality_id`, `carrier`, `rural_flag`, effective dates, `dataset_id`, and `dataset_digest`. | This is the right immediate target for RVU pricing smoke. |
| ZIP5 ingester | `CMSZipLocalityProductionIngester` has the source URL hard-coded and lands the ZIP. | Validation is mocked; normalization reads existing `cms_zip_locality LIMIT 1000` instead of parsing the raw ZIP; publish targets `cms_zip_locality`, not `geography`. |
| ZIP9 ingester | `CMSZip9Ingester` has real ZIP9 fixed-width parsing logic. | It filters to plus-four rows, hard-codes `2025-08-14`, drops carrier from the parsed output, maps rural values to booleans, and publishes to `zip9_overrides`, not `geography`. |
| Existing nearest ZIP models | `cms_zip_locality` and `zip9_overrides` exist for resolver/fallback architecture. | They are not the pricing resolver's current table and are not sufficient to remove the one-row seed dependency. |
| 2026 valuation smoke | RVU smoke validates `2026-07-01`. | A current 2026 ZIP-locality package was not confirmed in the source audit. The next implementation must either discover a newer CMS package or make latest-effective geography semantics explicit before using 2025Q4 rows for 2026 valuation dates. |

Decision:

Build a narrow, repeatable local/dev real geography loader for the next slice.
It should parse the CMS ZIP5 and ZIP9 source package directly into `geography`,
reuse existing local DB safety helpers, preserve source strings exactly, and
emit a load report with counts, digest, effective window, rejects, and coverage
metrics. Repairing the full DIS ZIP-locality and ZIP9 ingesters can follow, but
it should not block removing the one-row RVU smoke seed dependency because those
ingesters currently target different tables and have broader architectural
cleanup needs.

Recommended command shape for the next slice:

```bash
.venv/bin/python scripts/load_cms_geography_local.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing \
  --source-zip data/cms_raw/zip-code-carrier-locality-file-revised-08-14-2025.zip
```

Required loader behavior:

- refuse non-local databases unless `--allow-remote` is passed;
- default to non-destructive insert/register mode;
- support an explicit local/dev `--replace-existing` mode for the same
  `dataset_id` and digest/release only;
- compute `dataset_digest` from the source ZIP or normalized rows;
- write ZIP5 rows with `plus4=NULL`, `has_plus4=0`;
- write ZIP9 rows with `plus4=<source plus4>`, `has_plus4=1`;
- preserve state, carrier, locality, rural flag, ZIP5, and plus4 as source
  strings;
- fail rather than normalizing `00` to `01`;
- report row counts, rejected rows, duplicate active keys, state counts, ZIP5
  counts, ZIP9 counts, locality counts, and the `94110 -> CA/05/01112` probe;
- validate whether the effective-date window covers the RVU smoke valuation
  date before claiming that post-RVU smoke can run without the seed helper.

## Loader Slice Result

The harness selected `build-repeatable-geography-real-data-loader` after the
source audit. That slice is complete.

Added:

- `scripts/load_cms_geography_local.py`
- `tests/scripts/test_load_cms_geography_local.py`

The loader:

- parses `ZIP5_OCT2025.txt` and `ZIP9_OCT2025.txt` directly from the CMS ZIP
  package;
- preserves ZIP5, plus4, carrier, locality, state, rural flag, and CMS
  year/quarter as strings;
- writes ZIP5 rows to `geography` with `plus4=NULL`, `has_plus4=0`;
- writes ZIP9 rows to `geography` with source plus4 and `has_plus4=1`;
- computes a source ZIP SHA-256 digest;
- registers a `DatasetSnapshot` for the geography release;
- refuses non-local databases unless `--allow-remote` is passed;
- defaults to non-destructive behavior and requires `--replace-existing` before
  deleting overlapping local/dev geography rows;
- supports `--dry-run` source validation without a database;
- supports explicit `--open-ended-latest` local/dev semantics if the team wants
  the latest public ZIP-locality package to remain active beyond its source
  quarter;
- supports `--require-valuation-date-coverage` to turn valuation coverage gaps
  into a hard stop condition.

Focused tests cover fixed-width parsing, leading-zero preservation, locality
`00` preservation, duplicate active-key rejection, source dry-run reporting, and
the explicit latest-effective override.

Real-source dry-run command:

```bash
.venv/bin/python scripts/load_cms_geography_local.py \
  --dry-run \
  --report-json data/ingestion/local/reports/cms_geography_local_dry_run.json
```

Real-source dry-run result:

| Metric | Value |
|---|---:|
| Total rows | 1,118,970 |
| ZIP5 rows | 42,956 |
| ZIP9 rows | 1,076,014 |
| Rejected rows | 0 |
| Duplicate source keys | 0 |
| Locality `00` rows | 39,476 |
| Source effective window | 2025-10-01 to 2025-12-31 |
| Dataset digest | `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e` |
| `94110` probe | CA / locality `05` / carrier `01112` |
| Strict 2026-07-01 coverage | blocked |

Stop-condition run:

```bash
.venv/bin/python scripts/load_cms_geography_local.py \
  --dry-run \
  --require-valuation-date-coverage \
  --report-json data/ingestion/local/reports/cms_geography_local_dry_run_blocked.json
```

This correctly reports
`source_effective_window_does_not_cover_valuation_date` for the 2026-07-01 RVU
smoke date under strict source-quarter dating.

Next decision for the validation slice:

- if a newer 2026 CMS ZIP-locality package is found, load that package with
  strict effective dates;
- otherwise, explicitly choose whether local/dev RVU smoke may use
  `--open-ended-latest` for the latest public ZIP-locality package before
  replacing the single-ZIP seed helper.

## Validation And Smoke Result

The harness selected `validate-geography-breadth-and-locality-joins` after the
loader slice. That slice is complete.

Local/dev load command:

```bash
.venv/bin/python scripts/load_cms_geography_local.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing \
  --replace-existing \
  --open-ended-latest \
  --require-valuation-date-coverage \
  --report-json data/ingestion/local/reports/cms_geography_local_load_open_ended.json
```

Load result:

| Check | Result |
|---|---:|
| Existing seed rows deleted | 1 |
| Real geography rows inserted | 1,118,970 |
| ZIP5 rows | 42,956 |
| ZIP9 rows | 1,076,014 |
| Rejected rows | 0 |
| Duplicate active keys at 2026-07-01 | 0 |
| Active state/locality pairs | 119 |
| GPCI joins after MPFS special-state mapping | 119 |
| Missing GPCI joins after mapping | 0 |

The validation exposed CMS source-state codes that do not directly equal the
state values in the GPCI release: `AS`, `FM`, `GU`, `MH`, `MP`, and `PW` map to
GPCI state `HI`; `EK` and `WK` map to `KS`; `EM` and `WM` map to `MO`; and
`VA` locality `01` maps to the `DC` GPCI row for DC + MD/VA suburbs. The
implementation keeps `geography.state` source-native and applies this mapping
only in MPFS GPCI lookup.

The seedless post-RVU-load smoke passed:

```bash
.venv/bin/python scripts/post_rvu_load_api_smoke.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing
```

Result: `status=ok`, geography `state=CA`, `locality_id=05`,
`carrier=01112`, pricing `allowed_cents=11758`, and release
`rvu_2026_C`.

A special source-state smoke also passed:

```bash
.venv/bin/python scripts/post_rvu_load_api_smoke.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing \
  --zip 66012 \
  --expected-locality 00 \
  --expected-state EK \
  --expected-carrier 05202
```

Result: `status=ok`, geography `state=EK`, `locality_id=00`,
`carrier=05202`, pricing `allowed_cents=8902`, and release
`rvu_2026_C`.

The single-ZIP seed helper is no longer required for the normal local/dev RVU
post-load smoke. Keep it only as a legacy repair fallback when a developer needs
to patch a narrow local database without loading real geography breadth.

## Scope

In scope:

- Identify the exact public CMS ZIP-locality source package, files, layouts, and
  release metadata to use for the runtime `geography` table.
- Decide whether to repair the existing ZIP-locality/ZIP9 ingesters or add a
  narrow local/dev loader command that follows the same contracts.
- Parse real ZIP5 and ZIP9 rows, preserving ZIP, plus-four, state, carrier,
  rural flag, locality ID, effective date, and source digest.
- Load real rows into `geography` non-destructively by default, with explicit
  replace behavior only when requested.
- Register or document the snapshot/provenance row used by geography
  resolution.
- Validate coverage, uniqueness, leading-zero locality preservation, `00`
  preservation, and GPCI locality join coverage.
- Update the post-RVU-load smoke flow so geography preconditions come from real
  loaded geography data rather than the single-row seed helper.

Out of scope:

- Address-to-ZIP+4 enrichment.
- Private claims, patient, provider, customer, or production database data.
- Destructive production geography reloads.
- Replacing the RVU pricing implementation.
- Rebuilding the nearest-ZIP geospatial fallback unless real-source validation
  proves it is blocking breadth.

## Acceptance Criteria

- The source audit names the CMS ZIP-locality package, inner ZIP5/ZIP9 files,
  layout files, release ID, effective date rules, and target runtime table.
- A repeatable command can load real public CMS ZIP-locality data into local/dev
  `geography` without relying on a one-off mutation.
- The load is non-destructive by default and refuses non-local databases unless
  explicitly allowed.
- Loaded rows preserve leading zeros in ZIP5, plus4, carrier, and locality
  fields.
- Loaded rows never normalize CMS locality `00` to benchmark/default locality
  `01`.
- Active-row uniqueness is enforced or validated for `(zip5, plus4,
  valuation_date)`.
- Coverage checks report nationwide row counts, state counts, plus-four counts,
  ZIP5-only counts, and rejected/quarantined rows.
- Join validation proves loaded geography localities can join to loaded GPCI
  localities for the target valuation date.
- ZIP `94110` resolves to CA locality `05` from real loaded geography data, not
  from `scripts/seed_post_rvu_load_local.py`.
- The post-RVU-load API smoke passes after the real geography load and reports
  expected geography, pricing, positive allowed amount, release ID, and
  RVU/GPCI/CF trace refs.

## Validation

- `.venv/bin/python tools/work_tracker.py check`
- `.venv/bin/python -m pytest tests/geography/test_geography_resolver.py -q`
- `.venv/bin/python -m pytest tests/ingestors/test_cms_zip9_ingester.py tests/ingestors/test_zip9_ingester_services.py -q`
- A focused real-source geography loader test added or updated with fixture
  data from the CMS package layout.
- A local/dev real-load command that emits row counts, digest, effective window,
  rejects, and coverage metrics.
- `.venv/bin/python scripts/post_rvu_load_api_smoke.py --database-url <local-dev-postgres-url>`
  after the real geography load.

## Privacy / Data Boundaries

This epic uses public CMS geography and fee schedule data. The sanitizer gate is
normally a fast not-applicable check.

Stop for sanitizer review before implementation if a slice introduces private
customer/provider records, raw production database dumps, secrets, browser
state, finance/call artifacts, or external-memory ingestion.

## PRD / STD Impact

Update governed docs if implementation establishes a durable contract:

- `prds/PRD-geography-locality-mapping-prd-v1.0.md` for final ingestion,
  load, snapshot, and resolver behavior.
- `prds/REF-geography-source-map-prd-v1.0.md` or
  `prds/REF-cms-pricing-source-map-prd-v1.0.md` if CMS source URLs, file names,
  or layout assumptions change.
- `prds/SRC-locality.md` only for locality/county QA rules, not as the primary
  ZIP-to-locality source.

No new PRD is required before the source audit. A PRD/source-map update may be
required before closing the loader implementation if real CMS source behavior
differs from current docs.

## Known Risks

- CMS source URLs and file names may change across releases.
- Existing ZIP-locality ingester code appears to land the package but may replay
  seeded database data rather than parsing real source rows.
- ZIP5 rows can map to multiple localities; ZIP9 rows may be required to avoid
  ambiguous pricing in some areas.
- `localitycounty` and GPCI use MAC/locality geography, but they do not provide
  direct ZIP-to-locality breadth.
- Real-source row counts may expose schema mismatches in the current
  `geography` table or nearest-ZIP models.
- Coverage thresholds should fail clearly without forcing destructive local DB
  resets.

## Stop Conditions

- Stop if the authoritative CMS ZIP-locality source cannot be downloaded or
  identified from public source docs.
- Stop if the only available path requires private data, credentials, or
  production database access.
- Stop if implementation would overwrite existing geography rows without an
  explicit replace flag and a scoped local/dev database.
- Stop if real rows cannot preserve leading-zero ZIP, plus4, carrier, or
  locality values.
- Stop if implementation would normalize locality `00` to `01` without an
  explicit CMS rule.
- Stop if active-row uniqueness cannot be validated for ZIP/plus4 and valuation
  date.
- Stop if real geography localities cannot join to loaded GPCI localities for
  the target release; document whether the mismatch is source, schema, or
  effective-date related.
- Stop if validation fails for reasons outside the current slice.

## Ordered Task Slices

1. Audit geography real-data sources and gaps.
   - Status: done.
   - Tracker task:
     `state/work/tasks/audit-geography-real-data-sources-and-gaps.yaml`.
   - Evidence: source matrix, local row counts, gap list, target table decision,
     and narrow loader recommendation in this brief.
2. Build repeatable real geography loader.
   - Status: done.
   - Tracker task:
     `state/work/tasks/build-repeatable-geography-real-data-loader.yaml`.
   - Validation: fixture tests plus local/dev load report with counts, digest,
     rejects, effective window, `94110` probe, and locality preservation checks.
3. Validate geography breadth and locality joins.
   - Status: done.
   - Tracker task:
     `state/work/tasks/validate-geography-breadth-and-locality-joins.yaml`.
   - Validation: coverage metrics, active-row uniqueness, leading-zero and `00`
     preservation, and GPCI join checks.
4. Replace single-ZIP seed dependency with real geography smoke.
   - Status: done.
   - Tracker task:
     `state/work/tasks/replace-single-zip-seed-with-real-geography-smoke.yaml`.
   - Validation: post-RVU-load smoke passes after real geography load without
     relying on the 94110 seed helper.
5. Document geography real-data runbook and close RVU dependency.
   - Status: done.
   - Tracker task:
     `state/work/tasks/document-geography-real-data-runbook-and-close-rvu-dependency.yaml`.
   - Validation: docs and tracker state explain how to run real geography load
     before RVU smoke and what remains out of scope.

## Notes

Codex v0 ran this epic as the harness from audit through real local/dev load,
validation, and smoke. Future work should promote the local/dev loader behavior
into the production DIS ZIP-locality and ZIP9 ingesters.
