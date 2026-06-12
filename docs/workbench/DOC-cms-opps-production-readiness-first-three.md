# CMS OPPS Production Readiness First Three Tasks

**Status:** First three tracker slices complete locally
**Updated:** 2026-06-12
**Epic:** `state/work/epics/cms-opps-production-readiness.yaml`

## Scope

This note records the harness run for the first three OPPS production-readiness
tasks:

1. Audit OPPS source contracts and current ingestion.
2. Pin latest CMS OPPS source release.
3. Normalize OPPS Addenda and status indicator contracts.

No production or Render database was touched. Downloaded CMS ZIPs were used as
ignored local evidence under `data/ingestion/opps/` and are not intended for a
code PR.

## Task 1 Audit Findings

Current OPPS assets exist, but the production-readiness path was not complete:

- Source docs identify the CMS OPPS quarterly addenda page and describe
  Addendum A, Addendum B, and status indicator lookup expectations.
- `CMSOPPSScraper` has discovery/download support, including local sample
  handling and manifest storage.
- `OPPSIngestor` has the DIS stage scaffold, required-artifact profile checks,
  manifest/metadata emission, and parquet publishing.
- Before this run, `_parse_addendum_a`, `_parse_addendum_b`, and
  `_parse_zip_file` returned empty placeholder frames.
- Before this run, `_normalize_stage` wrote non-contract table keys
  `apc_payment` and `hcpcs_crosswalk` instead of `opps_apc_payment` and
  `opps_hcpcs_crosswalk`.
- Current CMS OPPS data includes two-character status indicators such as `E1`,
  `J1`, `Q1`, and `S1`; the ORM models and contracts only allowed
  one-character status indicators before this run.
- Addendum A has CMS prose preamble rows before the real header and uses
  non-UTF bytes in the official CSV; parser code must detect headers and use
  encoding fallback.
- Addendum B APC can be blank for some status indicators, so later validation
  must enforce referential integrity only for rows where CMS semantics require
  an APC reference.

Smallest safe implementation path:

- Parse official ZIP bundles by selecting Section 508 CSVs before XLSX files.
- Normalize Addendum A/B into contract table names before runtime/database
  writes.
- Preserve OPPS money as `Decimal` values during normalization.
- Expand status indicator width/domain before adding fail-fast SI validation.
- Leave request-context packaging and final allowed amount calculation for the
  later request-time resolver task.

## Task 2 Source Pin

Pinned source page:

- `https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates`

CMS states that Addendum A and B updates are posted quarterly and are snapshots
of HCPCS codes, status indicators, APC groups, and OPPS payment rates in effect
at the beginning of each quarter. The latest release listed on 2026-06-12 was
April 2026 for Addendum A, Addendum B, and Addendum Q.

Selected release:

- Release ID: `opps_2026q2_r1`
- Year/quarter: `2026 Q2`
- Effective window: `2026-04-01` through `2026-06-30`
- Addendum A page:
  `https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates/april-2026-addendum`
- Addendum A ZIP:
  `https://www.cms.gov/files/zip/cy-2026-april-opps-addendum.zip`
- Addendum B page:
  `https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates/april-2026-addendum-b`
- Addendum B ZIP:
  `https://www.cms.gov/files/zip/cy-2026-april-opps-addendum-b.zip`

Digest evidence:

| Artifact | SHA-256 |
| --- | --- |
| `cy-2026-april-opps-addendum.zip` | `32bee7d5e146074c2c5ee4586eaf1fc1b5bda56ccc7fa1737594179cb815f04e` |
| `cy-2026-april-opps-addendum-b.zip` | `127644907d9cc4026d7b8349400c7c3787290bffc28e40fecc96c32935b0b181` |

ZIP contents:

| ZIP | Member | Length |
| --- | --- | ---: |
| Addendum A | `2026 April Web Addendum A.03.23.26.xlsx` | 115276 |
| Addendum A | `508 Version April 2026 Web Addendum A/2026 April Web Addendum A.03.23.26.csv` | 105728 |
| Addendum B | `2026 April Web Addendum B.03.23.26.xlsx` | 3289834 |
| Addendum B | `508 Version April 2026 Web Addendum B/2026 April Web Addendum B.03.23.26.csv` | 1108293 |

Observed parser row counts from the pinned Section 508 CSVs:

| Table | Rows |
| --- | ---: |
| `opps_apc_payment` | 1037 |
| `opps_hcpcs_crosswalk` | 19057 |

Observed Addendum B status indicator domain:

`A`, `B`, `C`, `E1`, `E2`, `F`, `G`, `H`, `H1`, `J1`, `J2`, `K`, `K1`, `L`,
`M`, `N`, `P`, `Q1`, `Q2`, `Q3`, `Q4`, `R`, `S`, `S1`, `T`, `U`, `V`, `Y`.

## Task 3 Normalization Work Completed

Code changes:

- `OPPSIngestor._parse_addendum_a` now parses CSV/XLSX files with CMS preamble
  header detection and Decimal money parsing.
- `OPPSIngestor._parse_addendum_b` now parses CSV/XLSX files with CMS preamble
  header detection and preserves two-character status indicators.
- `OPPSIngestor._parse_zip_file` extracts Addendum A/B members and prefers
  Section 508 CSVs over workbook files.
- `_normalize_stage`, `_enrich_stage`, and ZIP parsing now use contract table
  keys: `opps_apc_payment`, `opps_hcpcs_crosswalk`, and
  `opps_rates_enriched`.
- OPPS status indicator model/contract width is now two characters.
- OPPS contracts and fallback schema include the status indicator domain
  observed in the pinned April 2026 Addendum B source.

Focused tests added:

- `tests/ingestors/test_opps_addenda_normalization.py`

Validation run:

```bash
/Users/alexanderbea/Cursor/cms-api/.venv/bin/python -m pytest tests/ingestors/test_opps_addenda_normalization.py -q
/Users/alexanderbea/Cursor/cms-api/.venv/bin/python -m pytest tests/ingestors/test_opps_required_files.py -q
/Users/alexanderbea/Cursor/cms-api/.venv/bin/python tools/work_tracker.py check
/Users/alexanderbea/Cursor/cms-api/.venv/bin/python tools/work_tracker.py check-views
```

Results:

- `tests/ingestors/test_opps_addenda_normalization.py`: 3 passed.
- `tests/ingestors/test_opps_required_files.py`: 3 passed.

## Remaining Risks

- `opps_apc_payment.relative_weight` is non-null in the contract, but official
  Addendum A has blank relative weights for some drug rows. A later validation
  slice should either make the contract nullable where CMS permits blanks or
  define a clear CMS-backed rule.
- `opps_hcpcs_crosswalk.apc_code` is nullable in code/contract, and Addendum B
  has blank APCs for some status indicators. Referential validation must be
  conditional on payable/APC-bearing statuses.
- Status indicator lookup population is still not complete; this task only
  aligns Addendum B source values with contract width/domain.
- Dataset snapshot registration, local DB persistence, and request-time OPPS
  pricing remain later tracker slices.
