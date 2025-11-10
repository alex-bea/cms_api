# STD-ingestor-config-prd-v1.0.md

doc_type: STD  
normative: true  
requires:
  - STD-data-architecture-impl-v1.0.md
  - STD-scraper-prd-v1.0.md

**Status:** Draft v1.0  
**Owners:** Data Platform Engineering  
**Consumers:** Ingestor authors (OPPS, MPFS, RVU, future ASC/NADAC pipelines)

**Cross-References:**  
- `prds/DOC-master-catalog-prd-v1.0.md` (master catalog registration)  
- `prds/STD-data-architecture-impl-v1.0.md` (DIS implementation patterns)  

---

## 0. Context & Purpose
Every DIS ingestor must know which artifacts (files, snapshots, manifests) are required for a given release. OPPS implemented a config-driven pattern (`ingestor_artifacts.yml`) plus a shared helper (`IngestorArtifactProfileService`). This standard codifies that pattern so other ingestors (MPFS, RVU, future pipelines) can declare required/optional artifacts, sandbox leniency, and release-specific overrides without hard-coding logic.

---

## 1. Configuration Schema (`cms_pricing/ingestion/config/ingestor_artifacts.yml`)

```yaml
datasets:
  <dataset_id>:
    default_profile: quarterly        # required
    baseline_profile: baseline        # optional
    baseline_regex: "_r00$"           # optional regex to detect baseline releases
    release_profiles:                 # optional map of regex → profile
      "_correction": correction
    sandbox:                          # optional flags for dev environments
      allow_missing_required: true
    profiles:
      quarterly:
        required: ["addendum_a", "addendum_b"]
        optional: ["addendum_q"]
      baseline:
        required: [...]
        optional: [...]
```

Rules:
1. `datasets` keys must align with DIS dataset IDs (`opps`, `mpfs`, `rvu`, ...).
2. Each profile lists the exact artifact types (file_type, snapshot, table) expected during land/validate stages.
3. Sandbox behavior (`allow_missing_required`) toggles validation from error → warning when engineers supply partial samples via `*_LOCAL_SAMPLE_DIR`.
4. Release routing chooses a profile in this order: explicit env override (`OPPS_INGESTION_PROFILE`), `release_profiles` regex match, `baseline_regex`, `default_profile`.
5. All artifact names used in required and optional lists MUST map to canonical ENUMs defined in REF-cms-pricing-source-map-prd-v1.0.md.
---

## 2. Reference Helper (`IngestorArtifactProfileService`)

Location: `cms_pricing/ingestion/services/ingestor_artifact_profile.py`

Usage pattern inside ingestors:
```python
service = IngestorArtifactProfileService()
profile = service.resolve(
    dataset="opps",
    release_id=batch_info.batch_id,
    profile_override=os.getenv("OPPS_INGESTION_PROFILE"),
    sandbox_mode=bool(self.local_sample_dir),
)
result = profile.validate(file_types)
```

- `resolve()` returns an `ArtifactProfile` with `.required`, `.optional`, `.allow_missing_required`.
- `.validate()` produces `{passed, errors, warnings, profile_name}` for the existing `required_files_present` validation rule.
- Ingestors should store the service on `self` during `__init__` and call it from validation.

---

## 3. Environment & Overrides

- `INGESTOR_ARTIFACT_CONFIG` (optional): path override for `ingestor_artifacts.yml` (defaults to repo path).
- `<DATASET>_INGESTION_PROFILE` (optional): force a specific profile (e.g., `OPPS_INGESTION_PROFILE=baseline`) for targeted testing.
- `<DATASET>_LOCAL_SAMPLE_DIR` (existing pattern): activates sandbox mode so missing required artifacts become warnings (when `allow_missing_required` is true).
- The IngestorArtifactProfileService MUST log the resolved profile name and the full list of required/optional artifacts to the IngestRun metadata for auditability.

Operators must document any overrides used during dry-runs and include the evidence JSON path in readiness tickets.

---

## 4. Adoption Guidance

1. **OPPS** — already uses the service for Addenda A/B/D1/E/Q. All sandbox dry-run evidence should include the resolved profile in `table_artifacts`.
2. **MPFS & RVU** — service instantiated but profiles are empty; ingestor owners should populate required snapshots/artifacts before enabling enforcement.
3. **Future ingestors (ASC, NADAC, etc.)** — new pipelines MUST declare their artifacts in `ingestor_artifacts.yml` and wire validation to the shared service before entering QA.

---

## 5. Compliance

- Validation rule `required_files_present` MUST rely on the profile definitions rather than hard-coded lists.
- Changes to `ingestor_artifacts.yml` require PR review from data platform owners.
- Sandbox leniency may only be enabled when `*_LOCAL_SAMPLE_DIR` is set; production runs must fail on missing required artifacts.
- All dataset keys MUST be defined in lower-case and MUST match the canonical DIS dataset ID.
- If allow_missing_required: true is enabled in a production run (i.e., when LOCAL_SAMPLE_DIR is NOT set), the ingestor MUST crash with Security/Compliance Error (EXIT_CODE_101).
