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
- **REF-pricing-calculator-prd-v1.0.md**

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

### Release Cadence & Naming
CMS publishes four planned RVU refreshes per calendar year plus ad-hoc correction notices. We align ingest scope and release identifiers to CMS suffixes so downstream lineage is deterministic.

| CMS Release | Expected Publish | DIS Release ID Examples | Notes |
|-------------|-----------------|-------------------------|-------|
| RVU**25A**  | Early January   | `rvu_2025_A`, `gpci_2025_A` | Final Rule baseline. |
| RVU**25B**  | April 1         | `rvu_2025_B`             | Mid-year update; overrides B quarter. |
| RVU**25C**  | July 1          | `rvu_2025_C`             | Mid-year update; overrides C quarter. |
| RVU**25D**  | October 1       | `rvu_2025_D`             | Mid-year update; overrides D quarter. |
| RVU**25AR/BR/CR/DR** | As needed | `rvu_2025_AR`, etc. | Correction notices; suffix keeps CMS naming. |

Implications for the ingestor:
- `MPFSIngestor.ingest(year, quarter)` normalizes `quarter`/suffix/`rvu_YYYY_SUFFIX` so operators can target the correct CMS release; defaults to the latest registered snapshot when no quarter is supplied.
- Snapshot discovery **must** prefer the requested release ID; if absent, the run should fail fast with instructions to ingest RVU first.
- RVU publish stage registers curated outputs with dataset-specific release IDs (`rvu_YYYY_S`, `gpci_YYYY_S`, `anescf_YYYY_S`, `locality_YYYY_S`, `oppscap_YYYY_S`). MPFS discovery therefore requests the matching prefix per dataset; shared release IDs are no longer accepted.
- Snapshot metadata persists manifest URLs for provenance, but `DatasetSnapshotService` resolves the actual parquet path before MPFS loads a dataset. Local manifest-only entries are acceptable as long as the referenced parquet exists on disk.
- If operators request a quarter (`Q2`, `B`, etc.) that does not exist in `dataset_snapshots`, the ingestor must fail fast with a `ValueError` instructing them to run the missing RVU ingestion first. MPFS is prohibited from silently falling back to “latest” when an explicit quarter is supplied.
- Metadata emitted by the MPFS run (batch manifest, observability record, `mpfs_cf_vintage`) includes `target_release_suffix`, `requested_release_param`, and the resolved RVU/GPCI release IDs to keep lineage auditable across quarters.

## API Readiness & Distribution
- **Curated Views:** `mpfs_rvu_latest`, `mpfs_gpci_latest`, and `mpfs_cf_current` provide Latest-Effective semantics for pricing services  
- **Digest Pinning:** APIs must accept `X-Dataset-Digest` / `?digest` matching curated manifest digests  
- **Access Controls:** Internal APIs behind token-based auth per **STD-api-security-and-auth-prd-v1.0.md**; pricing outputs include attribution if surfaced externally

## Objective  
Persist the full Medicare Physician Fee Schedule (MPFS) inputs — RVUs, policy/status indicators, Localities & GPCIs, and annual Conversion Factors — as authoritative datasets following the DIS pipeline.  
The ingestor performs validation, normalization, and provenance tracking but **does not compute payment amounts**.  
Payment calculations are executed on-request by the Pricing API using these curated inputs.
This design minimizes recomputation and decouples pricing logic from ingestion cadence.


## Scope  
- **Included**  
  - RVU quarterly files (A / B / C / D)  
  - National / abstract / payment files (when published)  
  - Locality & GPCI files  
  - Annual Conversion Factors (Physician + Anesthesia)  
  - All published policy / status / indicator columns  
  - Curated DIS input tables (`mpfs_rvu`, `mpfs_gpci`, `mpfs_cf_vintage`, etc.) for use by real-time calculators  

- **Excluded (in v1)**  
  - Sequestration adjustments  
  - Site-neutral transforms / overrides  
  - Pre-computed payment outputs (facility / non-facility amounts now computed dynamically)

- **Adjacent / references**  
  - PFS Look-Up Tool (used for API parity testing)  
  - OPPS Addendum B (future site-neutral analysis)

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
- `mpfs_link_keys` — minimal key set for downstream joins (hcpcs, modifier, quarter, locality_id)`  

> **Note:** MPFS ingestion no longer produces `mpfs_payment_curated`.  
> All payment computations are performed dynamically at request time.

## Keys & Joins  
- Primary key: `(hcpcs, modifier, quarter_vintage)`  
- Localities keyed by `(carrier_id, locality_code, year)`  
- ZIP linkage: use external ZIP → Locality resolver to derive pricing views (compute price downstream)  

## Quality / QC & Diffing  
- Schema presence checks (no dropped columns)  
- Idempotency (checksum, content type, consistent downloads)  
- Vintage lock (old versions immutable)  
- Diff reports: compare each new quarterly version with prior (additions, deletions, indicator changes)  

- Dashboards track the DIS five pillars (freshness, volume, schema, quality, lineage) with release-by-release diff summaries  
- Non-blocking parity review compares sampled HCPCS rows against the CMS PFS Look-Up Tool to confirm facility vs non-facility splits  

## Policy / Indicator Handling  
- Retain all status / indicator columns verbatim (global surgery, PC/TC, bilateral, multiple proc, etc.)  
- Document meaning of key indicators in annex (e.g. “Status Indicator A means …”)  

## Design Decisions  
- **Computation Timing:** The MPFS ingestor no longer computes payment amounts during ingestion.  
  Instead, it validates and stores authoritative input data (RVU, GPCI, Conversion Factor) for on-request calculation.  
- **Runtime Calculator Responsibility:** Payment formulas are applied by the Pricing API when requests are made, ensuring values remain current with CF or GPCI changes.  
- **Formula Reference:**  
  Payment = CF × [(Work_RVU × GPCI_Work) + (PE_RVU × GPCI_PE) + (MP_RVU × GPCI_MP)]  
- **Caching:** The Pricing API may cache computed prices per `(hcpcs, modifier, zip, year)` for performance.  
- **DIS Role:** The ingestor focuses purely on validation, lineage, and schema enforcement; no facility/non-facility calculations are materialized.  
- **Conversion Factor Scope:** v1 retains the Physician CF; anesthesia or mid-year factors may be added later via YAML or CLI overrides.

### Runtime Pricing Calculator (API Responsibility)
- The Pricing API performs MPFS payment calculations on-request using curated input datasets.  
- Facility vs. non-facility pricing is determined at runtime based on request context.  
- The calculator reads the latest curated inputs:
  - `mpfs_rvu` (RVUs + indicators)
  - `mpfs_gpci` (geography indices)
  - `mpfs_cf_vintage` (conversion factors)
- Results are cached for repeat queries to improve latency.  
- Parity with CMS Look-Up Tool is validated through API-level QA tests, not ingestion-time checks.

### Conversion Factor Governance (v1)
- Default behaviour derives the physician conversion factor directly from the registered RVU snapshot; CMS download/override is used only when a release publishes out-of-band CF guidance (e.g., correction notice).
- When CMS publishes a CF artefact, the fetcher downloads and caches under `data/ingestion/mpfs/raw/{year}`; metadata records the source URL or override path.
- Manual overrides are managed via the **YAML Config Service** (preferred) or **CLI flags** (emergency fallback). Overrides are keyed by release suffix (e.g., `mpfs_2025_B.yaml`) or year-level files with per-release sections.
- CF governance supports both calculator and batch audit use cases; ingestion records metadata, not computed values.


**YAML Config Service** (`cms_pricing/ingestion/services/mpfs_config_service.py`):
- Per-release configuration files: `cf_overrides/{release_id}.yaml`. Year-level files (e.g., `cf_overrides/mpfs_2025.yaml`) may contain a `releases:` map with entries such as `A`, `B`, `2025_Q2`, etc.; the service merges matching keys (suffix + quarter alias) so checksum/path overrides can be specified independently.
- YAML schema (top-level or under `releases` entries):
  ```yaml
  manual_override_path: "/path/to/cf_2025.xlsx"
  expected_checksum: "abc123def456..."
  ```
- Config service checks YAML first (if available), falls back to CLI flags if missing or invalid
- Config is cached in-memory for process lifetime (restart required for updates)
- Error handling: Missing YAML → WARN + fallback; Malformed YAML → Error with file path/line number + fallback
- When no override is supplied, ingestion logs `conversion_factor_strategy=derive_from_rvu` and records the RVU release ID used to compute CF rows; overrides switch the strategy to `download`.

**CLI Flags (Fallback)**:
- **IMPORTANT**: CLI flags remain the primary/fallback mechanism until YAML service is live and production-ready
- Pass `--cf-override-path` and `--cf-expected-checksum` when invoking ingestion script
- Once YAML service is stable, CLI flags become override/emergency only

**Migration Path**:
1. **Current State**: CLI flags are primary method
2. **After YAML Service Lands**: YAML config becomes primary, CLI flags become override/emergency only
3. **Recommended**: Store override files under `data/ingestion/mpfs/manual_overrides/` with restricted permissions

**Provenance**:
- Snapshot registry must capture override provenance by storing `manifest_url` or `curated_path` pointing to CF artefact used
- Override metadata (path + checksum) persisted on `mpfs_cf_vintage` snapshot metadata  

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
