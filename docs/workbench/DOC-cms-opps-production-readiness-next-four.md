# OPPS Production Readiness Next Four Evidence

Date: 2026-06-12

Epic: `cms-opps-production-readiness`

Tasks covered:

- `load-latest-cms-opps-local-db`
- `register-opps-snapshots-and-selection-tests`
- `add-opps-production-validation-gates`
- `implement-opps-request-time-pricing-resolver`

## Pinned Release

- Release ID: `opps_2026q2_r1`
- Effective window: `2026-04-01` through `2026-06-30`
- Source page: https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates
- Addendum A ZIP SHA-256: `32bee7d5e146074c2c5ee4586eaf1fc1b5bda56ccc7fa1737594179cb815f04e`
- Addendum B ZIP SHA-256: `127644907d9cc4026d7b8349400c7c3787290bffc28e40fecc96c32935b0b181`

## Local Load Evidence

Command:

```bash
/Users/alexanderbea/Cursor/cms-api/.venv/bin/python scripts/load_latest_cms_opps_local.py \
  --source-dir data/ingestion/opps \
  --output-dir data/ingestion/local/opps \
  --database-url sqlite:///data/ingestion/local/opps/opps_loader_smoke_v2.sqlite \
  --report-json data/ingestion/local/reports/cms_opps_local_load_latest_sqlite_smoke_v2.json
```

Observed output:

- `opps_apc_payment`: 1,037 rows
- `opps_hcpcs_crosswalk`: 19,057 rows
- `ref_si_lookup`: 28 rows
- Registered snapshots: `OPPS`, `opps_apc_payment`, `opps_hcpcs_crosswalk`, `ref_si_lookup`
- Snapshot effective window: `2026-04-01` through `2026-06-30`

Validation warnings accepted as CMS source-fidelity exceptions:

- Blank Addendum A payment rates appear only on non-separately payable APCs referenced by SI `H` or `H1`.
- Blank Addendum A relative weights are preserved because the CMS source contains those blanks.

## Validation Gates Added

The OPPS normalizer now fails before publish/load when:

- Addendum A or B is missing or below configured row floors.
- Required columns are absent.
- APC or HCPCS codes fail format checks.
- Addendum A has duplicate APC natural keys.
- Addendum B has duplicate HCPCS/modifier natural keys.
- Addendum B contains unknown status indicators.
- Addendum B references APC codes missing from Addendum A.
- Addendum A has negative payment rates.
- Addendum A has blank payment rates for APCs referenced by separately payable status indicators.

## Request-Time Resolver Behavior

The OPPS engine now first attempts normalized source-table pricing:

- Selects OPPS snapshot by valuation date.
- Resolves HCPCS/modifier from Addendum B.
- Resolves separately payable APC rates from Addendum A.
- Uses Decimal math and half-up cents rounding.
- Classifies SI values explicitly as payable, packaged, context-required packaged, not payable, or unknown.
- Raises on unknown SI or missing payable APC rates.
- Falls back to legacy OPPS tables only when normalized source-table lookup is unavailable or no source-table row exists.

Focused resolver coverage proves:

- `C1600` with SI `S` resolves through APC `5115` to `12346` cents for `123.456` USD.
- `C1601` with SI `Q1` returns zero allowed amount and `packaged=true` with a context-required trace ref.
- Snapshot selection uses CMS effective date `2026-04-01`, not ingestion run date.

## Validation Commands

```bash
/Users/alexanderbea/Cursor/cms-api/.venv/bin/python -m pytest \
  tests/ingestors/test_opps_addenda_normalization.py \
  tests/ingestors/test_opps_required_files.py \
  tests/ingestors/test_opps_local_load_and_resolver.py -q
```

Result: `10 passed`.

```bash
/Users/alexanderbea/Cursor/cms-api/.venv/bin/python scripts/load_latest_cms_opps_local.py \
  --parse-only \
  --source-dir data/ingestion/opps \
  --output-dir data/ingestion/local/opps \
  --report-json data/ingestion/local/reports/cms_opps_local_load_latest_parse_only.json
```

Result: parse-only validation passed with 1,037 APC rows and 19,057 HCPCS rows.
