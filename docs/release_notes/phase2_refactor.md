# Phase 2 Refactor – Release Notes (2025-02-14)

## Overview
Phase 2 reduces `RVUIngestor` from 4,247 lines to 990 lines (−76.7%) by extracting dataset-specific logic into reusable modules and centralizing shared services. The refactor introduces `DatasetSpec` contracts, modular stage helpers, and a shared `ServiceFactory`, enabling faster onboarding for new ingestors and consistent schema/validation behaviour.

## Highlights
- **DatasetSpec Registry** – `cms_pricing/ingestion/datasets/spec.py` encapsulates parser, schema, loader, validation, and routing metadata. RVU datasets register specs in `rvu_spec.py`, enabling declarative routing and business rules.
- **Modular Stage Helpers** – Land, Validate, Normalize, Enrich, and Publish stages now delegate to shared executors in `cms_pricing/ingestion/stages/`. RVUIngestor acts as a thin orchestrator.
- **Shared Services** – `ServiceFactory` provides lazy access to schema, validation, observability, quarantine, and reference-data services, eliminating duplicated setup across ingestors.
- **Schema Bootstrap & Caching** – `SchemaService` bootstraps RVU schema contracts once and caches them for validation, preserving performance guardrails.
- **Enrichment Fix** – The enrich stage now calls the shared reference-data pipeline, controlled via `ENABLE_ENRICHMENT`.
- **Legacy Compatibility Guards** – Land helper publishes both `raw_directory` (release root) and `raw_files_directory` (files payload) so Phase 0/1 tests keep working while the shared land executor remains unchanged.
- **Metadata Inference Guard** – `_build_parser_metadata()` falls back to `extract_vintage_metadata()`, ensuring filenames such as `PPRRVU25_JAN.txt` infer the correct quarter and avoid Render layout mismatches.

## Documentation Updates
- `prds/STD-parser-contracts-prd-v2.0.md` – Added DatasetSpec registry guidance and schema bootstrap expectations.
- `prds/STD-data-architecture-prd-v1.0.md` – Documented enrichment feature flag, partition requirements (`vintage_date`, `effective_from`), and shared stage modules/services.
- `prds/STD-data-architecture-impl-v1.0.md` – Added modular stage helper usage and DatasetSpec onboarding checklist.
- `artifacts/phase2_documentation_refresh_plan.md` – Tracks code deltas, documentation updates, verification status, and follow-up actions.

## Testing Status
- `python -m compileall cms_pricing/ingestion/ingestors/rvu_ingestor.py` ✅
- `pytest tests/ingestors/test_rvu_ingestor_e2e.py::TestRVUIngestorE2E::test_dis_validate_stage` ⚠️ Blocked by sandbox Signal 11 (documented in verification plans).

## Next Steps
- Propagate DatasetSpec/ServiceFactory patterns to MPFS/OPPS/ZIP9 ingestors.
- Update architecture diagrams to show stage executors and shared services.
- Monitor sandbox pytest issue; rerun E2E suite when resolved.

---

## Addendum (2025-11-04) – MPFS Snapshot-Based Ingestor
- **MPFS pipeline modernization:** `MPFSIngestor` now reuses RVU/GPCI snapshots, lands conversion factor artefacts via `ConversionFactorFetcher`, and computes facility/non-facility payments with `mpfs_builder`. Curated snapshots (`mpfs_payment_curated`, `mpfs_rvu`, `mpfs_gpci`, `mpfs_cf_vintage`, etc.) register provenance metadata automatically.
- **Documentation refresh:** Published dedicated runbook `prds/RUN-mpfs-ingestion-v1.0.md`, updated `prds/PRD-mpfs-prd-v1.0.md` with cross-join builder logic and conversion factor governance, refreshed gap analysis to mark MPFS as stable.
- **Testing follow-up:** End-to-end payment test passes; Phase 6 unit/contract tests (ConversionFactorFetcher, `/v1/mpfs datasets_used`) queued for completion alongside readiness run evidence.

## Addendum (2025-11-05) – PPRRVU 2025 Layout & Schema Alignment
- **2025 CMS layout support:** PPRRVU parser now backfills duplicate `RVU`/`PE RVU` headers introduced in `rvu25` bundles, normalizes indicator columns (blank → null), and casts OPPS cap flags to booleans. Structured logs (`pprrvu_multiheader_backfill`) surface when the repair runs in Render.
- **Schema contract updates:** Relaxed `status_code` domain to accept the full CMS vocabulary and realigned the locality Stage 1 schema to publish `mac`, `locality_code`, `state_name`, `fee_area`, `county_names` (FIPS remains a Stage 2 enrichment output). Contracts/reg docs updated accordingly.
- **Observability compatibility:** Added a `record_metric` shim to `DISObservabilityCollector` so enrichment stages emit metrics without raising `AttributeError` across legacy deployments.
