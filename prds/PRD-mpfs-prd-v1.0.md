# PRD: MPFS Ingest (RVU / GPCI / CF / Indicators)

**Status:** Draft v1.0 (Reviewed 2025-11-04)
**Owners:** Pricing Platform Product & Engineering  
**Consumers:** Data Engineering, Pricing API, Analytics, Ops  
**Change control:** ADR + PR review

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
- **STD-data-architecture-prd-v1.0.md:** Data ingestion lifecycle and storage patterns
- **STD-parser-contracts-prd-v2.0.md:** Parser core contracts (ParseResult, versioning)
- **STD-parser-contracts-impl-v2.0.md:** Parser implementation templates
- **STD-qa-testing-prd-v1.0.md:** Testing requirements for MPFS ingestion
- **REF-nearest-zip-resolver-prd-v1.0.md:** ZIP resolver for geography mapping
- **REF-cms-pricing-source-map-prd-v1.0.md:** Source inventory & work-backwards checklist

## Work-Backwards Checklist (Required)
Every MPFS ingester or schema change **must** trace back to **REF-cms-pricing-source-map-prd-v1.0.md**. Confirm the source table entry, authoritative layout, and checklist completion before authoring code or submitting review.

## Data Classification & Stewardship
- **Classification:** Public CMS release (Internal derived metrics and aggregations)  
- **License & Attribution:** CMS MPFS files (public domain); include CMS citation in manifests and curated docs  
- **Data Owner / Steward:** Pricing Platform Product (product owner), Data Engineering (technical steward)  
- **Downstream Visibility:** Curated tables are Internal; external pricing surfaces must pass compliance review and mask non-public enrichments

## Ingestion Summary (DIS v1.0)
- **Sources & Cadence:** Quarterly RVU A/B/C/D files, annual Locality & GPCI tables, annual Conversion Factor notices, CMS abstracts/policy files  
- **Schema Contracts:** `cms_pricing/ingestion/contracts/cms_pprrvu_v1.0.json`, `cms_gpci_v1.0.json`, `cms_localitycounty_v1.0.json`, `cms_anescf_v1.0.json`; versions pinned in manifests  
- **Landing Layout:** `/raw/mpfs/{release_id}/files/*` with `manifest.json` recording `source_url`, `fetched_at`, `release_id`, `sha256`, `license`, `notes_url`  
- **Natural Keys & Partitioning:** RVU tables keyed by `(hcpcs, modifier, quarter_vintage)`; locality/GPCI keyed by `(carrier_id, locality_code, valuation_year)`; curated snapshots partitioned by `vintage_date`  
- **Validations & Gates:** Structural (required files), schema contract enforcement, HCPCS format and effective-dating checks, indicator completeness, locality/GPCI join coverage ≥99.5%, quarter-over-quarter diff thresholds (±1% row drift)  
- **Quarantine Policy:** Failed records land in `/stage/mpfs/{release_id}/reject/` with rule code + payload; publish blocks on any critical errors or missing quarters  
- **Enrichment & Crosswalks:** Join to ZIP→Locality resolver for analytics keys; compute effective windows using valuation quarter and CMS effective_from metadata  
- **Outputs:** `/curated/mpfs/{vintage}/mpfs_rvu.parquet`, `mpfs_indicators_all.parquet`, `mpfs_locality.parquet`, `mpfs_gpci.parquet`, `mpfs_cf_vintage.parquet`, plus latest-effective views for API usage  
- **SLAs:** Land + publish ≤7 business days from CMS posting; manifest digests recorded; backfills re-run through identical validations  
- **Deviations:** None; any exceptions require ADR and update to this summary
- **Discovery Manifest & Governance:** MPFS ingestor uses snapshot-based discovery (reuses RVU/GPCI snapshots via `DatasetSnapshotService`) and `ConversionFactorFetcher` for CF artifacts. No dedicated MPFS scraper; discovery generates manifests recording snapshot reuse vs download entries. CI runs `tools/verify_source_map.py` so `REF-cms-pricing-source-map-prd-v1.0.md` stays synchronized with discovered artifacts.

## API Readiness & Distribution
- **Curated Views:** `mpfs_rvu_latest`, `mpfs_gpci_latest`, and `mpfs_cf_current` provide Latest-Effective semantics for pricing services  
- **Digest Pinning:** APIs must accept `X-Dataset-Digest` / `?digest` matching curated manifest digests  
- **Access Controls:** Internal APIs behind token-based auth per **STD-api-security-and-auth-prd-v1.0.md**; pricing outputs include attribution if surfaced externally

## Objective  
Persist the full Medicare Physician Fee Schedule (MPFS) dataset — RVUs, policy/status indicators, Localities & GPCIs, and annual Conversion Factors — and materialize curated payment outputs used by downstream **network + price + access** experiences. The ingestor now composes the authoritative `mpfs_payment_curated` view by blending RVU + GPCI + conversion factor inputs inside the enrichment stage.

## Scope  
- **Included**  
  - RVU quarterly files (A / B / C / D)  
  - National / abstract / payment files (when published)  
  - Locality & GPCI files  
  - Annual Conversion Factors (Physician + Anesthesia)  
  - All published policy / status / indicator columns  
  - Curated MPFS payment outputs (facility + non-facility) computed via cross-join builder  

- **Excluded (in v1)**  
  - Sequestration adjustments  
  - Site-neutral transforms / overrides  

- **Adjacent / references**  
  - PFS Look-Up Tool (for parity QC)  
  - OPPS Addendum B (for future site-neutral linkage)

## Sources & Cadence  
- **PFS Relative Value Files** (e.g. RVU25A/B/C/D) — quarterly refreshes  
- **PFS documentation / abstracts**  
- **Localities & GPCI** (yearly sets + occasional updates)  
- **Conversion Factors** (annually)  
- **Update cadence & vintage**  
  - Ingest CY 2023–2025 now; retain 6 years  
  - Monthly scraper to detect updates  
  - Freeze historical quarterlies; diff reports between versions  

### Primary CMS References  
- [CMS Physician Fee Schedule Relative Value Files](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files) — authoritative quarterly RVU bundles (A/B/C/D) and supporting documentation  
- [CMS Physician Fee Schedule Look-Up Tool](https://www.cms.gov/medicare/physician-fee-schedule/search/overview) — parity benchmark for sampled HCPCS pricing checks  
- [CMS Locality Key & GPCI Documentation](https://www.cms.gov/medicare/payment/fee-schedules/physician-fee-schedule/locality-key) — locality metadata and GPCI definitions used in geography joins  
- [CY 2025 Physician Fee Schedule Final Rule / Conversion Factor Release](https://www.cms.gov/newsroom/press-releases/hhs-finalizes-physician-payment-rule-strengthening-person-centered-care-and-health-quality-measures) — current conversion factor guidance  
- [How to Use the Medicare Physician Fee Schedule (CMS Booklet)](https://www.cms.gov/sites/default/files/2021-04/How_to_MPFS_Booklet_ICN901344_0.pdf) — official payment calculation methodology reference  

## Schema / Data Model  

### Raw tables (1:1 with CMS artifacts)  
- `rvu_raw_YYYYQ` — all columns from the corresponding RVU file  
- `pfs_abstract_raw_YYYYQ` — abstracts / national payment snippets  
- `locality_raw_YYYY` — locality metadata (carrier, locality, name, state, etc.)  
- `gpci_raw_YYYY` — GPCI indices (Work, PE, MP) by locality  
- `cf_raw_YYYY` — Physician & Anesthesia conversion factors  

### Curated / derived views  
- `mpfs_rvu` — core RVUs + indicators keyed by `(hcpcs, modifier, quarter_vintage)`  
- `mpfs_indicators_all` — exploded table of policy/indicator flags for analytics  
- `mpfs_locality` — locality dimension (id, code, name)  
- `mpfs_gpci` — GPCI indices per locality and vintage  
- `mpfs_cf_vintage` — CF values per year  
- `mpfs_payment_curated` — cross-join of RVU × GPCI with applied conversion factor producing facility/non-facility payments  
- `mpfs_link_keys` — minimal key set for downstream joins (hcpcs, modifier, quarter, locality_id)  

## Keys & Joins  
- Primary key: `(hcpcs, modifier, quarter_vintage)`  
- Localities keyed by `(carrier_id, locality_code, year)`  
- ZIP linkage: use external ZIP → Locality resolver to derive pricing views (compute price downstream)  

## Quality / QC & Diffing  
- Schema presence checks (no dropped columns)  
- Idempotency (checksum, content type, consistent downloads)  
- Vintage lock (old versions immutable)  
- Diff reports: compare each new quarterly version with prior (additions, deletions, indicator changes)  
- Parity sampling: recompute sample lines vs PFS Look-Up Tool / national values (non-blocking)  
- Dashboards track the DIS five pillars (freshness, volume, schema, quality, lineage) with release-by-release diff summaries  
- Non-blocking parity review compares sampled HCPCS rows against the CMS PFS Look-Up Tool to confirm facility vs non-facility splits  

## Policy / Indicator Handling  
- Retain all status / indicator columns verbatim (global surgery, PC/TC, bilateral, multiple proc, etc.)  
- Document meaning of key indicators in annex (e.g. “Status Indicator A means …”)  

## Design Decisions  
- Compute MPFS payment amounts during ingestion using `mpfs_builder.build_curated_views()` to guarantee consistent facility/non-facility outputs and provenance metadata.  
- Conversion Factor scope for v1 is **physician factor only**; anesthesia or mid-year adjustments require manual override plus governance approval.  
- Conversion factor overrides must be declared via configuration service (preferred) or CLI flags and recorded on the published `mpfs_cf_vintage` snapshot metadata.  
- Price formula aligns with CMS methodology:  
    Payment = Conversion_Factor × [ (Work_RVU × GPCI_Work) + (PE_RVU × GPCI_PE) + (MP_RVU × GPCI_MP) ]

### Conversion Factor Governance (v1)
- Default behaviour downloads CF artefact from CMS and caches under `data/ingestion/mpfs/raw/{year}`.  
- Manual overrides require: (a) storing artefact in secure path, (b) providing checksum, (c) documenting override in runbook + implementation plan.  
- Future configuration service will manage release-scoped overrides in `cf_overrides/{release_id}.yaml`; until then operators pass CLI flags (`--cf-override-path`, `--cf-expected-checksum`).  
- Snapshot registry must capture override provenance by storing `manifest_url` or `curated_path` pointing to CF artefact used.  

- Sequestration or other adjustments are applied after base price  
- Geographic floors / overrides handled as metadata flags (not transforms)  

## Licensing / ToS  
- CMS publishes these as open program data; include attribution in manifests  
- Record source URL, last-modified, robots/ToS metadata  

## Operations & Runbook Hooks  
- Follow `prds/RUN-mpfs-ingestion-v1.0.md` for quarter-end validation, including HCPCS spot checks against CMS RVU documentation and locality/GPCI sampling across MACs  
- Confirm conversion factors (physician and anesthesia) are pinned in `mpfs_cf_vintage` for the current calendar year before enabling downstream consumers  
- Capture release notes and supporting PDFs alongside manifests so operators can reference CMS change summaries during incident triage  

## Roadmap / Future Enhancements  
- Incorporate sequestration reductions downstream  
- Add flags / override logic for geographic floors (e.g. PE floor)  
- Site-neutral analytics via joining into OPPS data  
- Support retroactive updates or corrections (CMS errata)  

# PRD: MPFS Ingest (RVU / GPCI / CF / Indicators)

## Objective  
Persist the full Medicare Physician Fee Schedule (MPFS) inputs — RVUs, policy/status indicators, Localities & GPCIs, and annual Conversion Factors — to support downstream **network + price + access** and analytics use cases. The ingester stores everything; **no price math in-ingest** (computation lives downstream).

## Scope  
- **Included**  
  - RVU quarterly files (A / B / C / D)  
  - National / abstract / payment files (when published)  
  - Locality & GPCI files  
  - Annual Conversion Factors (Physician + Anesthesia)  
  - All published policy / status / indicator columns  

- **Excluded (in v1)**  
  - Price calculation (CF × (RVU × GPCI) etc.)  
  - Sequestration adjustments  
  - Site-neutral transforms / overrides  

- **Adjacent / references**  
  - PFS Look-Up Tool (for parity QC)  
  - OPPS Addendum B (for future site-neutral linkage)

## Sources & Cadence  
- **PFS Relative Value Files** (e.g. RVU25A/B/C/D) — quarterly refreshes  
- **PFS documentation / abstracts**  
- **Localities & GPCI** (yearly sets + occasional updates)  
- **Conversion Factors** (annually)  
- **Update cadence & vintage**  
  - Ingest CY 2023–2025 now; retain 6 years  
  - Monthly scraper to detect updates  
  - Freeze historical quarterlies; diff reports between versions  

## Schema / Data Model  

### Raw tables (1:1 with CMS artifacts)  
- `rvu_raw_YYYYQ` — all columns from the corresponding RVU file  
- `pfs_abstract_raw_YYYYQ` — abstracts / national payment snippets  
- `locality_raw_YYYY` — locality metadata (carrier, locality, name, state, etc.)  
- `gpci_raw_YYYY` — GPCI indices (Work, PE, MP) by locality  
- `cf_raw_YYYY` — Physician & Anesthesia conversion factors  

### Curated / derived views  
- `mpfs_rvu` — core RVUs + indicators keyed by `(hcpcs, modifier, quarter_vintage)`  
- `mpfs_indicators_all` — exploded table of policy/indicator flags for analytics  
- `mpfs_locality` — locality dimension (id, code, name)  
- `mpfs_gpci` — GPCI indices per locality and vintage  
- `mpfs_cf_vintage` — CF values per year  
- `mpfs_link_keys` — minimal key set for downstream joins (hcpcs, modifier, quarter, locality_id)  

## Keys & Joins  
- Primary key: `(hcpcs, modifier, quarter_vintage)`  
- Localities keyed by `(carrier_id, locality_code, year)`  
- ZIP linkage: use external ZIP → Locality resolver to derive pricing views (compute price downstream)  

## Quality / QC & Diffing  
- Schema presence checks (no dropped columns)  
- Idempotency (checksum, content type, consistent downloads)  
- Vintage lock (old versions immutable)  
- Diff reports: compare each new quarterly version with prior (additions, deletions, indicator changes)  
- Parity sampling: recompute sample lines vs PFS Look-Up Tool / national values (non-blocking)  

## Policy / Indicator Handling  
- Retain all status / indicator columns verbatim (global surgery, PC/TC, bilateral, multiple proc, etc.)  
- Document meaning of key indicators in annex (e.g. “Status Indicator A means …”)  

## Design Decisions  
- Do not compute pay amounts in the ingester; the ingester is strictly storage + light QC  
- Price formula (downstream) is:  
    Payment = Conversion_Factor × [ (Work_RVU × GPCI_Work) + (PE_RVU × GPCI_PE) + (MP_RVU × GPCI_MP) ]

- Sequestration or other adjustments are applied after base price  
- Geographic floors / overrides handled as metadata flags (not transforms)  

## Licensing / ToS  
- CMS publishes these as open program data; include attribution in manifests  
- Record source URL, last-modified, robots/ToS metadata  

## Roadmap / Future Enhancements  
- Incorporate sequestration reductions downstream  
- Add flags / override logic for geographic floors (e.g. PE floor)  
- Site-neutral analytics via joining into OPPS data  
- Support retroactive updates or corrections (CMS errata)  

## Architecture & Migration (Phase 2)

### Current State
The MPFS ingestor currently uses a monolithic structure. Future migration to the modular architecture pattern is planned.

### Migration to DatasetSpec Pattern (Planned)

The MPFS ingestor will follow the same modular architecture pattern as the RVU ingestor refactoring:

**Reference Implementation:** See `cms_pricing/ingestion/ingestors/rvu_ingestor.py` (990 lines, down from 4,247) as the template for migration.

**Migration Checklist:**
1. **Create DatasetSpecs** - Define `DatasetSpec` instances for each MPFS dataset (RVU, conversion factors, abstracts, locality, GPCI)
2. **Extract Loaders** - Move database loading logic to `datasets/mpfs_loaders.py`
3. **Extract Adapters** - Move parsing logic to `datasets/mpfs_adapter.py`
4. **Extract Business Rules** - Move complex validation to `DatasetSpec.business_rules`
5. **Use ServiceFactory** - Initialize shared services (SchemaService, ValidationService) via `ServiceFactory`
6. **Delegate to Stage Modules** - Replace inline stage logic with calls to `stages/execute_*` functions
7. **Target Line Count** - Achieve <1,000 lines (thin orchestrator pattern)

Adopt the RVU implementation patterns outlined in the next section—DatasetSpec registry, centralized service/bootstrap, modular stage execution, and thin orchestrator design—to keep MPFS aligned with the shared ingestion architecture.

**Reference Documentation:**
- `STD-data-architecture-impl-v1.0.md` §10 - Migration guide for existing ingestors
- `DOC-master-catalog-prd-v1.0.md` §10 - Key architectural patterns catalog
- `STD-database-platform-prd-v1.0.md` §6.1 - Loader pattern documentation
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - RVU implementation reference

### Proven Patterns from RVU

The RVU ingestor (`cms_pricing/ingestion/ingestors/rvu_ingestor.py`) serves as the reference implementation for MPFS migration. Key patterns to adopt:

#### Discovery Patterns

**RVU Implementation:** `rvu_ingestor.py:66-88, 252-434`

- **`_DiscoveryCallable` Wrapper:** Dual sync/async access that detects event loop. See **STD-data-architecture-impl-v1.0.md §1.8.1**.
- **Manifest-First Discovery:** Prefer local manifest files for offline/test runs before scraper.
- **Async-First with Sync Fallback:** Primary async discovery via scraper, fallback to manifest-based sync.

**MPFS Adoption:** MPFS uses snapshot-based discovery (reuses RVU/GPCI snapshots via `DatasetSnapshotService`) and `ConversionFactorFetcher` for CF artifacts. No dedicated MPFS scraper needed; discovery generates manifests recording snapshot reuse.

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Discovery Implementation
- **STD-data-architecture-impl-v1.0.md §1.8.1** - Discovery Callable Wrapper Pattern
- **REF-scraper-ingestor-integration-v1.0.md** - Discovery Manifest Contract

#### Initialization Patterns

**RVU Implementation:** `rvu_ingestor.py:139-197`

- **ServiceFactory Pattern:** Centralized service initialization with lazy loading. See **STD-data-architecture-impl-v1.0.md §2.6**.
- **Schema Pre-caching:** Cache schemas at init time for 5-10% performance improvement. See **STD-data-architecture-impl-v1.0.md §1.8.2**.
- **Business Rules Registration:** Auto-register validation rules from DatasetSpecs during init.

**MPFS Adoption:** Use `ServiceFactory` with `ServiceConfig` to initialize all services. Pre-cache MPFS schemas (`cms_mpfs_rvu`, `cms_mpfs_gpci`, `cms_mpfs_cf`) at initialization.

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Initialization Implementation
- **STD-data-architecture-impl-v1.0.md §2.6** - Component Initialization Pattern
- **STD-data-architecture-impl-v1.0.md §1.8.2** - Schema Pre-caching Optimization

#### Compatibility Patterns

**RVU Implementation:** `rvu_ingestor.py:201-226, 557-628`

- **RawBatch Coercion:** `_coerce_raw_batch_like()` handles dict-like objects from legacy tests. See **STD-data-architecture-impl-v1.0.md §1.8.3**.
- **Signature Flexibility:** Normalize stage accepts both `(validated_batch)` and `(validated_batch, raw_batch)` signatures.

**MPFS Adoption:** Implement similar compatibility helpers to support legacy tests and gradual migration.

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Compatibility Patterns Implementation
- **STD-data-architecture-impl-v1.0.md §1.8.3** - Compatibility Helpers

#### Enrichment Orchestration

**RVU Implementation:** `rvu_ingestor.py:663-824`

- **Multi-Dataset Processing:** Iterate over dataframes, process each dataset separately using `execute_enrich()`.
- **Empty DataFrame Handling:** Skip enrichment for empty datasets while preserving structure.
- **Metrics Aggregation:** Collect enrichment metrics per dataset, aggregate totals.

**MPFS Adoption:** Process MPFS datasets (RVU snapshots, GPCI snapshots, CF) separately, then aggregate metrics and reference data sources.

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Enrichment Orchestration Implementation
- **STD-data-architecture-impl-v1.0.md §1.8.4** - Enrichment Orchestration

#### Publish Stage Patterns

**RVU Implementation:** `rvu_ingestor.py:853-1011`

- **Schema Drift Detection:** Provide `drift_detector` callback to `execute_publish()`. See **STD-data-architecture-impl-v1.0.md §1.8.5**.
- **Database Loader Pattern:** Wrap dataset-specific loader in `loader_func` for `execute_publish()`. See **STD-data-architecture-impl-v1.0.md §1.8.8**.

**MPFS Adoption:** Implement `mpfs_loader_func` that wraps `load_mpfs_dataframes()` following the same pattern as RVU's `rvu_loader_func`.

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Publish Stage Implementation
- **STD-data-architecture-impl-v1.0.md §1.8.5** - Publish Stage Callbacks
- **STD-data-architecture-impl-v1.0.md §1.8.8** - Database Loader Function Pattern

#### Error Handling Patterns

**RVU Implementation:** `rvu_ingestor.py:1033-1045, 1047-1059`

- **Graceful Pipeline Failures:** Catch exceptions, return structured error response with `error_type`.
- **Empty Input Detection:** Only mark partial if no source files AND no files downloaded AND no records. See **STD-data-architecture-impl-v1.0.md §1.8.6**.

**MPFS Adoption:** Implement similar error handling with proper status preservation and empty input detection.

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Error Handling Implementation
- **STD-data-architecture-impl-v1.0.md §1.8.6** - Empty Input Detection
- **STD-data-architecture-impl-v1.0.md §1.8.7** - Error Handling Patterns

#### Migration Checklist Mapping

Map RVU patterns to MPFS migration checklist:

| Checklist Item | RVU Pattern Reference | Implementation Location |
|----------------|----------------------|------------------------|
| Create DatasetSpecs | **PRD-rvu-gpci-prd-v0.1.md §6.3** - Multi-Dataset Handling | `rvu_spec.py:132-235` |
| Extract Loaders | **PRD-rvu-gpci-prd-v0.1.md §6.3** - Publish Stage | `rvu_loaders.py` |
| Use ServiceFactory | **PRD-rvu-gpci-prd-v0.1.md §6.3** - Initialization | `rvu_ingestor.py:156-167` |
| Delegate to Stage Modules | **PRD-rvu-gpci-prd-v0.1.md §6.3** - Stage Delegation | `rvu_ingestor.py:638-661` |
| Target Line Count | **PRD-rvu-gpci-prd-v0.1.md §6.1** - Modular Architecture | 990 lines achieved |

**Note:** Single source of truth per CMS artifact; MPFS views **reference** RVU tables.

## Runbook Hooks  
- Primary operational guidance lives in `prds/RUN-mpfs-ingestion-v1.0.md`; follow it for execution scripts, CF override workflow, and artifact logging.  
- Spot-check 10–20 HCPCS+modifier per quarter against RVU pages.  [oai_citation:20‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu25a?utm_source=chatgpt.com)
- Validate locality/GPCI (5 samples across MACs).  [oai_citation:21‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician-fee-schedule/locality-key?utm_source=chatgpt.com)
- Confirm **CY2025 CF** pinned in `mpfs_cf_vintage`.  [oai_citation:22‡Centers for Medicare & Medicaid Services](https://www.cms.gov/newsroom/press-releases/hhs-finalizes-physician-payment-rule-strengthening-person-centered-care-and-health-quality-measures?utm_source=chatgpt.com)

## Licensing / ToS
CMS public program data; record attribution + robots/ToS in manifests.
