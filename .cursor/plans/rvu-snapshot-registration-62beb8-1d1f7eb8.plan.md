<!-- 1d1f7eb8-96c0-4a51-9473-f7376a2fc7b5 8a221020-5a52-4bf5-b2a6-e80a70643242 -->
# RVU → GPCI Snapshot Alignment Plan (v1.1, 2025-11-06)

## Goal

Ensure the RVU pipeline registers each curated dataset with a dataset-specific release ID (e.g. `gpci_2025_B`) and that snapshot metadata exposes the actual parquet locations so MPFS discovery can locate snapshots by quarter. Includes code updates, fallback, backfill strategy, validation, and operational rollout.

## Current State & Requirements

- RVU ingestor registers all datasets (`rvu_items`, `gpci_indices`, etc.) using the same base release ID (`rvu_YYYY_S`).
- Snapshot metadata currently stores the manifest path, so MPFS attempts to read JSON as Parquet during `_load_snapshot_dataframe`.
- MPFS discovery expects per-dataset release IDs (`rvu_YYYY_S`, `gpci_YYYY_S`, …); GPCI snapshots therefore go missing.
- Need deterministic mapping from base RVU release ID to dataset-specific release namespaces and real parquet paths in snapshot metadata.
- Recent failures (Nov 2025) confirmed that `DatasetSnapshotService.get_latest_snapshot()` resolves manifest.json instead of parquet; fallback handling and repair are required.

## Implementation Steps

### 1. Locate Snapshot Registration Logic

- File: `cms_pricing/ingestion/ingestors/rvu_ingestor.py`
- Review `_register_dataset_snapshots()` to confirm shared release ID usage and manifest-path registration.

### 2. Introduce Dataset-Specific Release Mapping

- Implement helper `_dataset_release_id(dataset_id: str, base_release_id: str) -> str` that derives dataset-specific release IDs.
- Mapping table:
| dataset_id       | Prefix     | Example target |
|------------------|------------|----------------|
| `rvu_items`      | `rvu`      | `rvu_2025_B`   |
| `gpci_indices`   | `gpci`     | `gpci_2025_B`  |
| `localitycounty` | `locality` | `locality_2025_B` |
| `anescf`         | `anescf`   | `anescf_2025_B` |
| `oppscap`        | `oppscap`  | `oppscap_2025_B` |
- Parse the base release ID once via simple string split to extract `{year}_{suffix}` and compose `{prefix}_{year}_{suffix}` per dataset.
- Default to returning the base release ID (with warning) if an unknown dataset surfaces (future proofing).

### 3. Fix Snapshot Metadata Paths

- In RVU publish stage, after manifest generation:
- Load manifest JSON.
- For each dataset, retrieve `parquet_path` from `manifest["datasets"][dataset_name]["parquet_path"]`.
- Pass this parquet path as `path` to `register_snapshot()`.
- Keep `manifest_url` pointing to manifest; only `path` changes.
- Add structured logging showing dataset, base release ID, dataset-specific release ID, and parquet path.
- Update `tests/ingestors/test_rvu_ingestor_e2e.py` to assert `SnapshotMetadata.path` matches the dataset’s parquet file.
- **Temporary repair step:** Run a one-time utility to patch existing snapshot rows where `path` points to `manifest.json`, replacing it with the correct parquet path from the manifest. This should run in a networked environment before MPFS rerun.

### 4. Add Manifest Fallback in MPFS

- File: `cms_pricing/ingestion/ingestors/mpfs_ingestor.py`
- Update `_load_snapshot_dataframe()` to detect `.json` snapshot paths:
```python
if snapshot_meta["path"].endswith(".json"):
with open(snapshot_meta["path"]) as f:
manifest = json.load(f)
parquet_path = manifest["datasets"][snapshot_meta["dataset_id"]]["parquet_path"]
return pd.read_parquet(parquet_path)

•	This prevents ingestion failure if snapshot metadata drifts and still points to manifests.
•	Keep this as a safety net until all snapshots store parquet paths.

5. Update Snapshot Registration Calls
•	With dataset-specific release IDs and real parquet paths, call register_snapshot() per dataset.
•	Ensure release ID mapping helper is used and manifest URL remains consistent.

6. Backfill Strategy
•	Historical backfill optional: current production has no prior snapshots; future re-registration script can be added if missing historical data causes MPFS mismatch.
•	Defer backfill until stable ingestion is confirmed.

7. Testing
•	Extend RVU E2E tests to verify:
•	Dataset-specific release IDs (e.g. gpci_2025_B) are passed to register_snapshot().
•	Snapshot metadata path points to actual parquet file.
•	Add regression coverage:
•	Test where snapshot.path points to a manifest.json and ensure fallback loads parquet successfully.
•	Verify that after RVU publish fix, DatasetSnapshotService.get_latest_snapshot() returns .parquet paths and dataset-specific release IDs.

8. Documentation Updates
•	prds/RUN-mpfs-ingestion-v1.0.md & prds/PRD-mpfs-prd-v1.0.md:
•	Document dataset-specific release naming.
•	Explain that RVU snapshots now record parquet paths (not just manifest URLs).
•	artifacts/mpfs_implementation_plan.md:
•	Add note to validate dataset-specific release IDs and parquet path metadata.
•	Operational checklist: Before running MPFS ingestion, confirm all snapshot paths end with .parquet; if not, run scripts/repair_snapshot_paths.py to fix them.

9. Rollout
•	Run repair script to patch existing snapshots.
•	Deploy MPFS fallback to ensure ingestion stability.
•	Implement and deploy RVU publish-stage fix for dataset-specific release IDs and parquet paths.
•	Run unit/e2e tests locally.
•	Deploy to staging, ingest latest release (e.g. B), and confirm:
•	dataset_snapshots contains dataset-specific release IDs with parquet paths.
•	MPFS ingestion and discovery succeed.
•	Backfill production snapshots only if necessary after validation.

10. Monitoring & Validation
•	Add a simple weekly check to ensure gpci_indices release IDs (with gpci_ prefix) mirror rvu_items release IDs by suffix.
•	Optional: CI lint to flag future calls to register_snapshot() that use base release ID instead of helper.

⸻

Change Log
•	v1.1 (2025-11-06): Integrated learnings from manifest-path failure. Added repair script step, MPFS manifest fallback, regression test coverage, operational checklist, and clarified rollout sequencing.
•	v1.0 (2025-11-04): Original alignment plan with per-dataset release IDs and snapshot metadata path correction.

---

✅ This updated version:

- Resolves every “Key Learning” point.  
- Adds repair + fallback before MPFS rerun.  
- Keeps MVP scope minimal and achievable (no schema changes).  
- Tightens test and rollout sequencing for immediate stability.

### To-dos

- [ ] Audit `_register_dataset_snapshots` in `cms_pricing/ingestion/ingestors/rvu_ingestor.py` to confirm shared release ID usage
- [ ] Implement dataset-specific release ID helper and update snapshot registration calls with logging
- [ ] Decide on and implement backfill strategy for existing gpci snapshots (script or migration)
- [ ] Extend RVU + MPFS tests to assert dataset-specific release IDs and discovery success
- [ ] Document naming changes in runbook, PRD, and MPFS implementation plan
- [ ] Execute staging and production rollout steps, including monitoring script