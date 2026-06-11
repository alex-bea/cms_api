# CMS Geography Production Ingester Contract Audit

**Status:** Complete
**Updated:** 2026-06-11
**Tracker task:** `state/work/tasks/audit-geography-production-ingester-contracts.yaml`
**Epic:** `state/work/epics/cms-geography-production-ingestion.yaml`

## Purpose

Compare the proven local/dev CMS ZIP-locality loader against the existing
production ZIP-locality and ZIP9 ingestion paths before changing production
ingester code.

This slice answers four questions:

1. Which parsing behavior is already proven and should be reused?
2. Which production ingester contracts currently diverge from runtime
   geography needs?
3. Which target table and provenance semantics are required for RVU pricing?
4. What is the smallest safe implementation path for the next slice?

## Decision

Use the local/dev loader behavior as the source of truth for CMS ZIP5 and ZIP9
fixed-width parsing, reporting, duplicate-key detection, effective-window
calculation, locality `00` preservation, and runtime `geography` row shape.

The next slice should extract those parser/reporting primitives into shared
ingestion code used by both `scripts/load_cms_geography_local.py` and the
production ingester path. It should not change publish targets yet.

Publishing real CMS ZIP-locality rows into runtime `geography` should happen in
the following slice, after the parser module has focused tests and stable
return objects.

## Evidence Map

| Area | Evidence | Finding |
|---|---|---|
| Proven parser row shape | `scripts/load_cms_geography_local.py:73` | `ParsedGeographyRow` already carries source file/line, ZIP5, plus4, `has_plus4`, state, carrier, locality, rural flag, year/quarter, and effective dates. |
| Runtime geography mapping | `scripts/load_cms_geography_local.py:93` | The local loader already maps parsed rows into `geography` columns, including `dataset_id` and `dataset_digest`. |
| ZIP5 parsing | `scripts/load_cms_geography_local.py:325` | ZIP5 fixed-width parsing validates 80-character lines, state, 5-digit ZIP, 5-digit carrier, 2-digit locality, plus-four flag, and source year/quarter. |
| ZIP9 parsing | `scripts/load_cms_geography_local.py:372` | ZIP9 fixed-width parsing preserves ZIP5, plus4, carrier, locality, and source year/quarter, and requires plus-four flag `1`. |
| Source member selection | `scripts/load_cms_geography_local.py:421` | The local loader requires exactly one ZIP5 text file and exactly one ZIP9 text file in the CMS package. |
| Source validation/reporting | `scripts/load_cms_geography_local.py:468` | The local loader detects rejects, duplicate source keys, counts ZIP5/ZIP9 rows, tracks locality counts, and captures probe rows. |
| Valuation/effective reporting | `scripts/load_cms_geography_local.py:582` | The report includes effective window, valuation coverage, locality `00` counts, digest, source files, and the 94110 probe result. |
| Non-destructive local load | `scripts/load_cms_geography_local.py:664` | The loader refuses overlapping rows unless scoped replace is explicit. |
| Snapshot registration | `scripts/load_cms_geography_local.py:717` | The loader registers a `DatasetSnapshot` keyed by dataset and release with digest/effective window/provenance URL. |
| Runtime insert | `scripts/load_cms_geography_local.py:758` | Rows are bulk inserted into runtime `geography` in batches. |
| Runtime resolver target | `cms_pricing/services/geography.py:219` | Pricing geography resolution queries runtime `Geography` for ZIP+4 first, then ZIP5. |
| Runtime geography model | `cms_pricing/models/geography.py:9` | `geography` has the required runtime fields: ZIP5, plus4, `has_plus4`, state, locality, carrier, effective dates, dataset ID, and digest. |
| Legacy ZIP5 table | `cms_pricing/models/nearest_zip.py:51` | `cms_zip_locality` exists, but it has ZIP5-only primary-key shape and boolean rural flag, not runtime ZIP+4-first geography shape. |
| Legacy ZIP9 table | `cms_pricing/models/nearest_zip.py:85` | `zip9_overrides` exists for range override behavior, but it is not the current pricing resolver target. |

## Gap Matrix

| Capability | Proven local/dev behavior | Production ZIP-locality behavior | Production ZIP9 behavior | Gap |
|---|---|---|---|---|
| Source package discovery | Uses the local CMS package path and requires ZIP5/ZIP9 text members. | Hard-codes the same CMS URL. | Hard-codes the same CMS URL. | No current source discovery or newest-release selection. Keep hard-coded package for MVP, but isolate URL/source metadata behind a shared source descriptor. |
| Land raw package | Local loader expects a local file; no network. | Downloads and writes raw ZIP plus manifest. | Downloads and writes raw ZIP plus manifest. | Production land stage is useful and should be reused, but parser should accept landed bytes/path rather than querying DB. |
| Validate raw structure | Parses actual source rows and fails on rejected rows or duplicate active keys. | `_validate_data` returns mocked success. | Parses ZIP9 and runs validator, but skips incomplete rows instead of surfacing reject counts. | Production ZIP-locality validation must call the shared parser scan. ZIP9 should stop silently dropping malformed rows. |
| Parse ZIP5 | Parses `ZIP5_*.txt` fixed-width rows into runtime geography shape. | Does not parse landed source; `_normalize_data` reads `cms_zip_locality LIMIT 1000`. | Not applicable. | Move ZIP5 parsing to shared module and call it from production normalize/validate. |
| Parse ZIP9 | Parses `ZIP9_*.txt` fixed-width rows into runtime geography shape with carrier and plus4 preserved. | Not applicable. | Parses ZIP9 rows but outputs `zip9_low`/`zip9_high`, omits carrier from output, and hard-codes dates to 2025-08-14. | Shared ZIP9 parser must preserve carrier and source quarter, then adapters can project either runtime geography or legacy ZIP9 override rows. |
| Effective dates | Derives quarter windows from source `year_quarter`, with explicit `open_ended_latest` override. | Uses whatever existing `cms_zip_locality` rows contain. | Hard-codes `effective_from` and `vintage` to 2025-08-14 with open-ended `effective_to`. | Source quarter is the canonical default. Latest-effective/open-ended semantics must remain explicit policy, not hidden in production parser. |
| Rural flag | Preserves source string on runtime `geography`. | Maps `A`/`B` to boolean `True`. | Maps `A`/`B` to boolean `True`. | Runtime geography needs source-native string preservation. Legacy table adapters may continue converting if required by their schema. |
| Locality `00` | Preserved as a valid CMS locality value and counted in reports. | No explicit gate. | Validator allows two digits, so `00` passes, but no preservation gate. | Add explicit regression gate that `00` remains `00` and is never normalized to `01`. |
| Target table | Writes runtime `geography`, which pricing resolver reads. | Publishes to `cms_zip_locality`. | Publishes to `zip9_overrides`. | Production ingestion must publish to runtime `geography` for RVU pricing readiness. Legacy table publication can remain secondary/out of scope. |
| Provenance | Uses `dataset_id=ZIP_LOCALITY`, source digest, report metadata, and `DatasetSnapshot`. | Uses ingestion run metadata and per-row file checksum in `cms_zip_locality`; no runtime `DatasetSnapshot` for `ZIP_LOCALITY`. | Uses ingestion run metadata and generated checksum in `zip9_overrides`; no runtime `DatasetSnapshot` for `ZIP_LOCALITY`. | Production path needs `DatasetSnapshot` registration for the runtime geography release. Prefer `DatasetSnapshotService.register_snapshot` or equivalent transaction-safe service usage. |
| Replace semantics | Non-destructive by default; scoped replace only when explicit. | Inserts into `cms_zip_locality`; no scoped runtime geography replace semantics. | Inserts into `zip9_overrides`; unique range index can fail on repeat load. | Runtime publish needs dry-run, non-destructive default, and explicit scoped replace/reload for dataset/effective-window overlap. |
| Validation report | Emits source facts, row counts, rejects, duplicates, locality counts, probe rows, and valuation coverage. | Reports mocked quality and row counts from database sample. | Reports validator result and curated artifacts, but not row-count parity with ZIP5/ZIP9 source package. | Shared report object should become the validation contract for later production gates. |

## Contract Findings

### Runtime Contract

The pricing path depends on `geography`, not `cms_zip_locality` or
`zip9_overrides`. `GeographyService` queries `Geography` for exact ZIP+4 rows
with `has_plus4 == 1`, then exact ZIP5 rows with `has_plus4 == 0`
(`cms_pricing/services/geography.py:219` and
`cms_pricing/services/geography.py:249`).

That means the production ingestion path is not production-ready for RVU
pricing until it can publish both ZIP5 and ZIP9 source rows into
`cms_pricing.models.geography.Geography`.

### ZIP-Locality Production Ingester

`CMSZipLocalityProductionIngester` has a useful land stage:

- it downloads the CMS ZIP-locality package;
- writes a raw file;
- records source URL, hash, size, content type, and fetch timestamp.

The validate/normalize/publish stages are not equivalent to the proven local
loader:

- validation is currently mocked (`cms_zip_locality_production_ingester.py:204`);
- normalization reads existing `cms_zip_locality` rows with `LIMIT 1000`
  instead of parsing the landed source (`cms_zip_locality_production_ingester.py:234`);
- publication inserts `CMSZipLocality` rows
  (`cms_zip_locality_production_ingester.py:286`), not runtime `Geography`;
- rural flag conversion loses source-native `A`/`B` shape by converting both to
  boolean `True` (`cms_zip_locality_production_ingester.py:261`).

### ZIP9 Ingester

`CMSZip9Ingester` is closer to real source parsing but still diverges from the
runtime geography contract:

- its output contract is `zip9_overrides`
  (`cms_zip9_ingester.py:99`);
- it parses only ZIP9 rows and projects them as exact `zip9_low`/`zip9_high`
  ranges (`cms_zip9_ingester.py:399`);
- it reads carrier from fixed-width source but drops it from the emitted row
  (`cms_zip9_ingester.py:411`);
- it hard-codes effective/vintage dates to 2025-08-14 instead of deriving the
  quarter window from the source year/quarter (`cms_zip9_ingester.py:431`);
- it converts rural flag to boolean (`cms_zip9_ingester.py:443`);
- it enriches by querying `cms_zip_locality`, not by using the same source
  package scan (`cms_zip9_ingester.py:526`);
- it publishes to `ZIP9Overrides`
  (`cms_zip9_ingester.py:548`).

This ingester can contribute adapter/publish patterns, but its parser should be
replaced by or routed through the shared source parser.

### Existing Validators

The current validators cover useful field-level constraints, but not enough of
the production source contract:

- ZIP-locality validator has ZIP5/state/locality/date/uniqueness checks
  (`cms_zip_locality_validator.py:101`), but it is tied to ZIP5-style data and
  the current production ingester does not use it on real source rows.
- ZIP9 validator checks ZIP9 format, range, state, locality, rural flag,
  dates, ZIP5-prefix consistency, and completeness
  (`zip9_overrides_validator.py:36`), but it validates a projected
  `zip9_overrides` dataframe, not the canonical source-row object.

The next implementation should keep these validators as table-specific gates
where useful, but shared source parsing should own fixed-width field validation,
reject accounting, duplicate active-key detection, `00` preservation, and
quarter-window derivation.

## Smallest Safe Implementation Path

1. Extract shared source parser primitives from
   `scripts/load_cms_geography_local.py` into an ingestion module, likely under
   `cms_pricing/ingestion/cms_geography_source.py` or
   `cms_pricing/ingestion/parsers/cms_geography.py`.
2. Keep the local/dev script as a thin CLI wrapper around the shared module.
3. Add focused parser tests before changing production publish behavior:
   ZIP5 parse, ZIP9 parse, leading zeros, locality `00`, source member
   selection, reject reporting, duplicate active-key rejection, source digest,
   quarter effective window, and explicit open-ended/latest behavior.
4. After parser tests pass, update production ZIP-locality validation/normalize
   to call the shared parser against the landed ZIP package.
5. Only then add runtime `geography` publication with non-destructive default,
   explicit scoped replace, and `DatasetSnapshot` registration.

## Shared Parser Contract For Next Slice

The shared parser should expose stable, testable primitives:

- `ParsedCMSGeographyRow` or equivalent immutable row object;
- `CMSGeographySourceStats` or equivalent scan/report object;
- `source_zip_digest(path_or_bytes)`;
- `source_members(path_or_bytes)`;
- `iter_source_rows(path_or_bytes, open_ended_latest=False)`;
- `scan_source_zip(path_or_bytes, dataset_id, digest, probe_zip, open_ended_latest=False)`;
- adapter helpers for runtime `geography` mappings;
- optional adapter helpers for legacy `cms_zip_locality` and `zip9_overrides`
  projections only after the runtime path is stable.

The parser must not:

- normalize locality `00` to `01`;
- coerce source ZIP, plus4, carrier, locality, or rural flag into lossy types;
- silently skip malformed rows without accounting for rejects;
- infer open-ended/latest semantics unless explicitly requested;
- write to any database.

## Effective-Date Policy

Default behavior should remain strict source-quarter dating:

- source `year_quarter=20254` maps to `2025-10-01` through `2025-12-31`;
- rows are open-ended only when an explicit workflow option requests
  latest-effective semantics;
- production publish should fail or report blocked when the selected source
  cannot cover the requested valuation date and latest-effective mode was not
  selected.

The source discovery task can later replace this policy with a newer package if
CMS publishes one. Until then, `open_ended_latest` is an explicit operational
choice, not a parser default.

## Stop Conditions Carried Forward

Stop implementation if:

- the parser cannot preserve leading-zero ZIP5, plus4, carrier, or locality
  strings;
- locality `00` is normalized away;
- production validation still reads existing DB rows instead of the landed
  source package;
- runtime `geography` publication would delete or overwrite rows without an
  explicit scoped replace flag;
- `DatasetSnapshot` registration cannot be made idempotent for the selected
  release/digest;
- production code would write to a non-local database without an approved
  runbook.

## Next Task Handoff

Activate `extract-shared-cms-geography-parser`.

The task should be limited to shared parsing/reporting extraction plus tests.
It should not yet publish to `geography`, alter production database writes, or
change RVU smoke behavior.
