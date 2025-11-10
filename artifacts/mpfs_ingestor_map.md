# MPFS Ingestor — Current State Map

## 0. Overview
`cms_pricing/ingestion/ingestors/mpfs_ingestor.py` implements the Medicare Physician Fee Schedule (MPFS) pipeline. It predates the shared ServiceFactory, so dependencies are wired manually. This map captures the current behavior per stage and highlights deviations from DIS standards.

## 1. Initialization
- **Services wired:** `DatasetSnapshotService`, `ConversionFactorFetcher`, `MPFSConfigService`, `HistoricalDataManager`, `ValidationEngine`, `QuarantineManager`, `DISObservabilityCollector`, `ReferenceDataManager`, `SchemaRegistry`.
- **Session handling:** Uses `SessionLocal` directly if no DB session provided (not ServiceFactory-based).
- **Contract/SLA:** `contract_schema_ref = "cms.mpfs:v1.0"`; SLA = 24h processing max, 120h freshness alert, quality ≥0.99, availability 0.999.
- **Curated outputs:** `mpfs_rvu`, `mpfs_indicators_all`, `mpfs_locality`, `mpfs_gpci`, `mpfs_cf_vintage`, `mpfs_link_keys`.

## 2. Land Stage (`land_stage`, ~line 521)
- **Authoritative inputs:** RVU (PPRRVU release), GPCI indices, Conversion Factor CSV.
- **How resolved:** `DatasetSnapshotService` fetches RVU/GPCI releases; `ConversionFactorFetcher.ensure_conversion_factor` pulls CF artifact into `raw/`.
- **Release sync rules:** `_derive_release_suffix` enforces matching RVU/GPCI suffix (A–D); mismatches trigger validation failure.
- **RawBatch metadata:** includes `release_id`, `batch_id`, timestamps, `source_files` array with checksums and manifest info.

## 3. Validate Stage (`validate_stage`)
- Runs structural + schema + domain rules via `ValidationEngine`.
- Drift guardrails include ±15% row count comparison against previous release.
- Failures push batch to quarantine (see `QuarantineManager`).

## 4. Normalize Stage (`normalize_stage`)
- Builds `MPFSNormalizedInputs` by joining RVU + GPCI + CF data.
- Applies governance metadata (release_id, effective dates, manifest URLs) to each table.
- Prepares frames for `build_curated_views` in `cms_pricing/ingestion/datasets/mpfs_builder.py`.

## 5. Publish Stage (`publish_stage`)
- Calls `build_curated_views` to emit curated parquet tables and link keys.
- Persists `metadata.json` with `digest`, `release_id`, `batch_id`, `manifest_url`, file checksums.
- Updates observability metrics (freshness, volume, schema, quality, lineage).

## 6. Provenance & Caching
- Digest + release_id tracked per run; historical copies kept under `historical/` via `HistoricalDataManager`.
- Downstream services detect new release IDs to invalidate caches (ingestor does not do so automatically).

## 7. Readiness Questions
| # | Question | Answer |
|---|----------|--------|
| 1 | Authoritative inputs? | RVU (PPRRVU), GPCI indices, Conversion Factor CSV. |
| 2 | Required schema/contract? | `cms.mpfs:v1.0`; parsers must emit matching columns. |
| 3 | Release synchronization? | `_derive_release_suffix` ensures RVU/GPCI share suffix (A–D). |
| 4 | Conversion factor fallback? | `ConversionFactorFetcher.ensure_conversion_factor`; fails hard if CF missing. |
| 5 | Where calculations happen? | At request time (pricing calculator). Ingestion only stores inputs. |
| 6 | Persisted fields? | Work/PE/MP RVUs, policy indicators, locality info, GPCI components, conversion factors, link keys. |
| 7 | Provenance tracking? | `metadata.json` + table artifacts store digest, release_id, manifest_url, checksums. |
| 8 | Validation thresholds? | Structural rules + ±15% row drift via ValidationEngine. |
| 9 | SLA targets? | Processing ≤24h, freshness alert 120h, quality ≥0.99, availability 0.999. |
|10 | Schema/observability metrics? | Recorded via `DISObservabilityCollector`. |
|11 | Downstream consumers? | Pricing Calculator/API uses curated MPFS tables. |
|12 | Cache/digest invalidation? | New release IDs trigger downstream cache invalidation (manual/external). |

## 8. Outstanding gaps / deviations
- Manual dependency wiring (no ServiceFactory/shared snapshot loader).
- No graceful fallback when CF missing; ingestion aborts.
- Documentation (PRD/runbook) not aligned with current wiring; refactor plan should follow this map.
