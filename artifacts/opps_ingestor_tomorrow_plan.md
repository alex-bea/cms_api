# OPPS Ingestor Focus Plan (Tomorrow)

**Date:** 2025-11-06  
**Owner:** Alexander  
**Objective:** Move the OPPS ingestor (`cms_pricing/ingestion/ingestors/opps_ingestor.py`) from scaffold to a minimally working pipeline that can parse Addenda A/B, enrich with wage index data, and publish to the OPPS tables.

---

## 0. Pre-Day Prep (optional, today evening)
- Download the latest OPPS quarterly package (ZIP + Section 508 CSVs/XLSX) into `data/raw/opps/samples/` so parsing can happen offline tomorrow.
- Skim `artifacts/opps_implementation_plan.md` to refresh parsing edge cases and column mappings; note any TODOs that should be prioritized tomorrow.

---

## 0.1 Calculation Mode Agreement
- Document that OPPS ingestion only performs value-added calculations (e.g., wage-index adjustments, drift stats) when explicitly requested. The default run should land raw Addendum A/B outputs as-is.
- Add a work item tomorrow to introduce a runtime switch (CLI flag or config on `OPPSIngestor`) such as `calculate_on_request`, guarding `_enrich_stage` paths that compute `opps_rates_enriched` and any future derived metrics (`cms_pricing/ingestion/ingestors/opps_ingestor.py:601`–`631`).
- When calculations are requested, ensure the pipeline logs clearly that enrichment/derived tables are being produced and that downstream consumers understand these are computed artifacts rather than source-of-truth columns.
- Capture this behavior in the README/plan so requestors know calculations require the flag and won’t run automatically during standard ingestion.

---

## 1. Kickoff & Context Reload (09:00–09:30)
1. `git status && git pull` to ensure the working tree matches main.
2. Re-open `cms_pricing/ingestion/ingestors/opps_ingestor.py` and scan the five stages (`ingest_batch`, `_land_stage`, `_validate_stage`, `_normalize_stage`, `_enrich_stage`, `_publish_stage`) to note any TODO comments or placeholders introduced since last review.
3. Confirm sample data paths in `self.raw_dir`/`self.stage_dir` match the downloaded files; delete any stale temp files under `./data/ingestion/opps/tmp` if needed.
4. Create a scratch checklist in `artifacts/tomorrow_plan.md` referencing this document.

---

## 2. Data Acquisition & Land Stage Validation (09:30–10:30)
- Run `_land_stage` in isolation via an interactive script/notebook to ensure `CMSOPPSScraper.discover_latest()` returns metadata with `year`/`quarter` needed downstream (`opps_ingestor.py:470-518`).
- If the scraper cannot hit CMS (network restricted), seed `quarter_files` manually by instantiating `ScrapedFileInfo` objects that mirror the metadata used in production.
- Verify downloaded files include at least:
  - Addendum A CSV/XLSX
  - Addendum B CSV/XLSX
  - Quarterly HCPCS update notes (for SI validation later)
- Record precise column headers from each sample file; this informs the normalization helpers in Step 3.

Deliverable: cached dataset in `raw/opps/<batch_id>/files/` plus a short note on actual vs. expected filenames.

---

## 3. Parser Implementations (10:30–13:00)
### 3.1 Addendum A (APC Rates)
- Implement `_parse_addendum_a` (`opps_ingestor.py:888`) to handle CSV, XLSX (multiple sheet names), and fixed-width TXT if present.
- Create `_normalize_addendum_a_columns` helper that maps observed headers to canonical fields: `apc_code`, `apc_description`, `relative_weight`, `payment_rate_usd`, `packaging_flag`.
- Derive `effective_from`/`effective_to` using `_calculate_effective_window` logic introduced earlier in the file.
- Add unit-ish coverage by calling the coroutine with the sample file and asserting non-empty DataFrame with correct dtypes.

### 3.2 Addendum B (HCPCS Crosswalk)
- Fill `_parse_addendum_b` (`opps_ingestor.py:897`) similar to A, but include modifier/status-indicator normalization plus handling for repeated headers and trailing summary rows.
- Implement `_normalize_addendum_b_columns` with HCPCS padding, modifier trimming, SI uppercase enforcement, and optional payment context mapping.
- Wire `_parse_addendum_b` into `_normalize_stage` so the combined payload feeds `normalized_data['hcpcs_crosswalk']` (check call site at `opps_ingestor.py:566-607`).

### 3.3 Wage Index / SI Lookup
- Inspect `_enrich_stage` (`opps_ingestor.py:608-665`) to confirm how `GeographyEnricher` expects inputs; implement `_enrich_data` to join Addendum A outputs with wage index reference data (likely from `cms_pricing/models/fee_schedules.py`).
- Populate `_create_default_si_schema` and ensure SI lookup table is loaded via `SchemaRegistry` (lines just after `_load_schema_contracts`).

Break for lunch after verifying both parser helpers successfully read the sample files end-to-end.

---

## 4. Validation Stage Build-Out (13:30–15:00)
- Implement the placeholder validators `_validate_apc_codes`, `_validate_status_indicators`, `_validate_payment_rates`, `_validate_apc_cross_reference`, `_validate_hcpcs_existence`, `_validate_temporal_uniqueness`, `_validate_row_count_drift`, `_validate_rate_drift`, `_validate_coverage_drift` (`opps_ingestor.py:806-885`).
  - Use `ValidationEngine` where possible; otherwise, compute simple DataFrame checks and return structured results (`{"passed": bool, "errors": [...]}`).
- Update `_setup_validation_rules` to register these validators with meaningful severities/explanations.
- Ensure failures route batches to `_quarantine_batch` with actionable error text; test by purposefully corrupting a sample file.

---

## 5. Publish & Persistence Wiring (15:00–16:00)
- Confirm `_publish_stage` writes to the correct parquet paths (`self.curated_dir`) and, if database URL is configured, inserts into SQLAlchemy models (`cms_pricing/models/opps/*`).
- Validate schema compatibility between DataFrame columns and model definitions (e.g., `payment_rate_usd` numeric precision) cited in:
  - `cms_pricing/models/opps/opps_apc_payment.py`
  - `cms_pricing/models/opps/opps_hcpcs_crosswalk.py`
  - `cms_pricing/models/opps/opps_rates_enriched.py`
- If needed, add an adapter that converts DataFrame rows into model dictionaries before bulk insert.

---

## 6. Testing & Tooling (16:00–17:30)
1. **Unit / Parser Tests**
   - Add targeted tests under `tests/ingestion/test_opps_ingestor.py` (new file) that mock `ScrapedFileInfo` and feed sample CSVs into `_parse_addendum_a/_b`.
2. **End-to-End Dry Run**
   - Create a small async script under `scripts/run_opps_ingestion.py` (or reuse `dis_pipeline.py`) to call `OPPSIngestor.ingest_batch("opps_2025q1_r01")` against the sample data with `database_url=None`.
   - Verify logs show progression through land→publish and curated parquet files land in `curated/opps/<batch_id>/`.
3. **Observability Snapshot**
   - Ensure `self.observability` receives metrics (record `validation_results`, `publish_results` contents at `opps_ingestor.py:443`).

Document test commands and results in `artifacts/opps_ingestor_tomorrow_plan.md` under a new **"Results"** section before EOD.

---

## 6.1 Schema Alignment Checklist (New)

### I. Implementation Breakdown
| Item | Primary Files | Secondary Files | Summary |
| --- | --- | --- | --- |
| Canonical table keys | `cms_pricing/ingestion/ingestors/opps_ingestor.py` (`_normalize_stage`, `_enrich_stage`, `_publish_stage`, validators) | `tests/ingestors/test_opps_ingestor_e2e.py`, `tests/fixtures/opps/opps_dataset_creator.py` | Rename dict keys from `apc_payment`/`hcpcs_crosswalk` to `opps_apc_payment`/`opps_hcpcs_crosswalk` so outputs match schema contracts. |
| Enrichment/publish references | same file sections as above | `cms_pricing/routers/opps.py` (read-only verification), SQLAlchemy models | Ensure wage-index enrichment, CPT masking, and parquet filenames use canonical names. |
| Default schema fallback | `_create_default_opps_schema`, `_create_default_si_schema` | Contract JSON files for reference (`cms_pricing/ingestion/contracts/*.json`) | Update fallback dicts to mirror version 1.1 contracts (columns, business rules, metadata). |
| Plan visibility | `artifacts/opps_ingestor_tomorrow_plan.md` | `artifacts/ingestor_gap_analysis.md` (optional) | Keep this checklist in the plan to track progress. |

### II. Dependency Checklist
- Key rename cascades through `_normalize_stage → _enrich_stage → _publish_stage`, validator helpers, and tests/fixtures.
- Pseudo-code for rename:
```python
TABLE_APC = "opps_apc_payment"
TABLE_HCPCS = "opps_hcpcs_crosswalk"

if file_info.file_type == "addendum_a":
    normalized_data[TABLE_APC] = await self._parse_addendum_a(file_info)

if TABLE_APC in enriched_data:
    enriched_data["opps_rates_enriched"] = await self._enrich_with_wage_index(
        enriched_data[TABLE_APC], wage_index_data
    )
```

### III. Testing & Validation Guidance
- Update OPPS e2e tests to assert canonical table names (e.g., `assert "opps_apc_payment" in publish_results["tables_published"]`).
- Regenerate any fixture manifests if table lists are embedded (`pytest tests/fixtures/opps --maxfail=1`).
- Run `pytest tests/ingestors/test_opps_ingestor_e2e.py::TestOPPSIngestor::test_opps_ingester_schema_contracts` plus a dry-run ingestion to confirm parquet filenames.

### IV. Guardrails & Risk Mitigation
- Risk: mismatched table names break DIS contracts/downstream consumers.
- Mitigation: run schema-contract tests, inspect parquet filenames under `curated/opps/<batch>` before committing, and ensure fallback schema versions match the JSON contract to avoid silent regressions.

---

## 7. Wrap-Up & Next-Day Prep (17:30–18:00)
- Update this plan with actual outcomes, blockers, and follow-ups (e.g., open questions about SI lookup source, schema deltas that need PRD updates).
- File TODOs/issues for anything that slipped (e.g., need for CLI flags, additional validators).
- Commit code if stable; otherwise, stash and leave a brief summary in `artifacts/tomorrow_plan.md` for the following day.

---

### References
- `cms_pricing/ingestion/ingestors/opps_ingestor.py`
- `cms_pricing/models/opps/opps_apc_payment.py`
- `cms_pricing/models/opps/opps_hcpcs_crosswalk.py`
- `cms_pricing/models/opps/opps_rates_enriched.py`
- `artifacts/opps_implementation_plan.md`
