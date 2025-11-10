STD-metadata-governance-prd-v1.0.md
===================================

**Status:** Draft v1.0  
**Owner:** Platform / Data Engineering  
**Scope:** All DIS-compliant scrapers, ingestors, and dataset snapshot services  
**Purpose:** Document the metadata artifacts the codebase emits today so engineers can reason about lineage, validation, and downstream contracts without reverse‑engineering each pipeline.

**Cross-References:**  
- `prds/DOC-master-catalog-prd-v1.0.md` — Master catalog registration  
- `prds/STD-data-architecture-prd-v1.0.md` — DIS lifecycle anchor  

---

1. Overview
-----------

DIS produces three metadata artifacts on every run:

1. **Discovery Manifests** (scraper output; codified in `cms_pricing/ingestion/metadata/discovery_manifest.py`)  
2. **Land Manifests** (ingestor `land` stage; created by `BaseDISIngestor._create_manifest`)  
3. **Publish Metadata** (curated output manifest + `dataset_snapshots` rows; produced in `cms_pricing/ingestion/stages/publish.py` and `cms_pricing/services/dataset_snapshot_service.py`)

Dataset PRDs may define additional governance fields, but they must do so as documented extensions layered on top of these foundations. This STD replaces the placeholder schema that mentioned `update_type`, `precedence_rank`, etc., because those fields do not exist in the shipped code today.

---

2. Artifact Matrix
------------------

| Artifact | Produced By | Storage / Path | Consumers | Required Contract Section |
|----------|-------------|----------------|-----------|---------------------------|
| Discovery Manifest | Scrapers (`CMSRVUScraper`, `CMSOPPSScraper`, etc.) | `DiscoveryManifestStore` under `data/<dataset>/manifests` | `DiscoveryManifestStore.load_latest`, `tools/verify_source_map.py`, manual review in PRDs | §3 |
| Land Manifest | `BaseDISIngestor._create_manifest` (automatically called inside land stage) | `raw/<dataset>/<run>/manifest.json` | Pipeline observability, local debugging, dataset snapshot registration | §4 |
| Publish Metadata | `publish.py` manifest + dataset_snapshot rows | `curated/<dataset>/.../manifest.json`, Postgres `dataset_snapshots` | APIs, dataset selector, Pricing service lineage | §5 |
| Governance Extensions | Dataset-specific logic (currently OPPS plans) | Embedded in dataset payloads | Future schema versions / ADRs | §6 |

---

3. Discovery Manifest Contract
------------------------------

Implementation reference: `DiscoveryManifest` + `DiscoveryFileEntry` in `cms_pricing/ingestion/metadata/discovery_manifest.py`.

### 3.1 Required root fields

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `source` | string | `cms_rvu` | dataset identifier for scraper |
| `source_url` | string (URL) | `https://www.cms.gov/.../pfs-relative-value-files` | canonical landing page |
| `discovered_from` | string (URL) | same as `source_url` or detail page | tracked for parity checks |
| `discovered_at` | ISO 8601 datetime | `2025-10-04T07:21:08.735160+00:00` | assigned when manifest is created |
| `files` | array[DiscoveryFileEntry] | — | see 3.2 |
| `metadata` | object | `{"scraper_version": "1.6.0"}` | optional, but always serialized |
| `extras` | object | `{}` | reserved for dataset-specific metadata |

### 3.2 DiscoveryFileEntry fields

| Field | Type | Required | Example | Notes |
|-------|------|----------|---------|-------|
| `url` | string (URL) | ✅ | `https://www.cms.gov/files/zip/rvu25a.zip` | original download URL |
| `filename` | string | ✅ | `rvu25a.zip` | defaulted from URL |
| `content_type` | string | ✅ | `application/zip` | autopopulated using extension map |
| `size_bytes` | integer | optional | `4055697` | set once download occurs |
| `sha256` | string | optional | `8871f4...` | derived during download or manifest import |
| `year` | integer | optional | `2025` | convenience for filtering |
| `quarter` | string | optional | `A` | dataset-specific |
| `file_type` | string | optional | `zip` / `txt` etc. |
| `last_modified` | ISO 8601 datetime | optional | `Fri, 10 Jan 2025 18:54:27 GMT` |
| `metadata` | object | optional | `{"detail_url": "...", "posted_at": "2025-01-10"}` | arbitrary key/value pairs |

Validation helper: `DiscoveryManifest.validate()` asserts that every manifest contains `source`, `source_url`, `discovered_at`, `files[]`, and every file entry contains `url`, `filename`, `content_type`. CI should call this helper (or reuse its logic) instead of ad-hoc `jq`.

---

4. Land Manifest Contract
-------------------------

Implementation reference: `BaseDISIngestor._create_manifest` in `cms_pricing/ingestion/contracts/ingestor_spec.py`.

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `release_id` | string | `rvu_2025_test_1761711932` | stable identifier for this CMS release |
| `batch_id` | string (UUID) | `3f325b40-347b-4475-ae94-b770481d8f8c` | generated per run with `uuid4()` |
| `dataset_name` | string | `cms_rvu` | ingestor identifier |
| `dis_version` | string | `1.0` | DIS spec version stored in code |
| `tool_version` | string | `1.0.0` | ingestor version |
| `fetched_at` | ISO datetime | `2025-10-28T21:25:32.943751` | manifest creation time |
| `source_files` | array | — | see below |
| `license` | object | `{"name": "CMS Open Data","url": "...","attribution_required": true}` | defaulted |

Each entry in `source_files` carries:

| Field | Description |
|-------|-------------|
| `url`, `filename`, `content_type` | same meaning as discovery manifest |
| `size_bytes` | expected size if known |
| `last_modified` | ISO datetime, optional |
| `etag` | HTTP ETag if provided |
| `checksum` | sha256 of landed file when available |

Land manifests live next to raw downloads (e.g., `test_data/ingestion_2025/.../manifest.json`) and are used by tests (`tests/ingestors/test_mpfs_manifest_fallback.py`) when running offline. No other fields are required.

---

5. Publish Metadata + Dataset Snapshots
---------------------------------------

Publish stage metadata is emitted twice:

1. **Curated manifest** (`curated/<dataset>/manifest.json`) describing output parquet files.  
2. **Dataset snapshot row** inserted into Postgres table `dataset_snapshots` to give APIs deterministic lineage.

### 5.1 Publish manifest (from `cms_pricing/ingestion/stages/publish.py`)

| Field | Type | Example |
|-------|------|---------|
| `dataset_name` | string | `cms_rvu` |
| `release_id` | string | `rvu_2025_test_1761711932` |
| `batch_id` | string | `3f325b40-347b-4475-ae94-b770481d8f8c` |
| `vintage_date` | string (YYYY-MM-DD) | `2025-10-28` |
| `generated_at` | ISO datetime | `2025-10-28T22:01:55Z` |
| `datasets` | array | Each entry has `name`, `records`, `parquet_path` |

Additional documents—`dataset_documentation.json`, schema contract dumps, latest-effective view SQL—are part of the publish directory but are not standardized further here.

### 5.2 Dataset snapshot schema (`cms_pricing/models/dataset_snapshots.py`)

| Column | Type | Notes |
|--------|------|-------|
| `dataset_id` | string (PK part 1) | e.g., `rvu_items`, `gpci_indices` |
| `release_id` | string (PK part 2) | matches publish manifest `release_id` |
| `digest` | string (64-char SHA256) | checksum of curated parquet contents |
| `effective_from` | date | when snapshot becomes active |
| `effective_to` | date nullable | when superseded |
| `manifest_url` | string | local path or remote URL pointing to publish manifest |
| `created_at` | timestamp | server default `NOW()` |

`DatasetSnapshotService` optionally dereferences `manifest_url` to locate actual parquet paths (see `_resolve_curated_path`). Any new metadata the STD introduces must be persisted either via this manifest file or new DB columns (requires migration + ADR).

---

6. Governance Extensions (Dataset-Specific)
-------------------------------------------

Fields like `update_type`, `precedence_rank`, `provenance_link`, `effective_date`, `conversion_factor`, and `manifest_id` apply to OPPS/MPFS governance work tracked in `artifacts/opps_ingestor_tomorrow_plan.md` and `prds/PRD-opps-prd-v1.0.md`. They are **not emitted today** and therefore remain optional extensions until the corresponding code changes land.

Guidance:

1. Document extensions in the dataset PRD and reference this STD.  
2. Implementers must add the fields to the publish manifests and/or curated tables, plus expand `dataset_snapshots` or downstream APIs if those fields affect selection logic.  
3. Adding a mandatory field requires an ADR and a version bump (e.g., `STD-metadata-governance-prd-v1.1`) so existing ingestors remain compliant.

---

7. Validation & Tooling
-----------------------

| Check | Tool / Location | Notes |
|-------|-----------------|-------|
| Discovery manifest structure | `DiscoveryManifest.validate()` in `cms_pricing/ingestion/metadata/discovery_manifest.py` | call this in CI to ensure required fields |
| Documentation parity | `tools/verify_source_map.py` | ensures manifests referenced in REF docs actually exist |
| Publish manifest integrity | `tests/ingestors/test_rvu_ingestor_e2e.py::test_snapshot_registration_stores_parquet_paths` | confirms manifest URLs resolve |
| Snapshot path resolution | `tests/services/test_dataset_snapshot_service.py::test_resolve_path_from_manifest_dataset_list` | demonstrates expected `manifest_url` contents |

Recommended jq guardrails (example for discovery manifest):

```bash
jq -e '
  .source and .source_url and .discovered_at and
  (.files | length > 0) and
  ([.files[].url] | all(. != null))
' data/manifests/cms_rvu/*.jsonl
```

---

8. Compliance Statement
-----------------------

Every dataset PRD must reference this document and specify which artifacts it emits (Discovery, Land, Publish, Snapshot) plus any approved extensions. When new metadata becomes mandatory, publish an ADR, update this STD, bump its version, and link the commit in the PRD.

---

End of Document (Metadata Governance Standard — DIS v1.0 alignment)
