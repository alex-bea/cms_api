<!-- 1d1f7eb8-96c0-4a51-9473-f7376a2fc7b5 9ec0f19c-9511-4d46-9641-459ab3bcab89 -->
# RVU Snapshot Documentation Updates

## Overview
Update remaining documentation (3/5 items) to reflect completed RVU snapshot registration implementation, including dataset-specific release IDs, operational tools, and accurate metrics.

## Current Status
**Actual line count:** 1,383 lines (verified via `wc -l`)
**Reduction:** 67.4% from original 4,247 lines
**Target:** <1,500 lines ✅ ACHIEVED

**Already Complete (2/5):**
- ✅ `prds/RUN-mpfs-ingestion-v1.0.md` - Section 1.2 documents RVU snapshot auto-registration
- ✅ `prds/PRD-mpfs-prd-v1.0.md` - Line 54 notes dataset-specific release IDs

**Remaining (3/5):**
- ❌ `prds/PRD-rvu-gpci-prd-v0.1.md` - Missing snapshot registration section
- ❌ `prds/RUN-global-operations-prd-v1.0.md` - Missing snapshot verification steps  
- ❌ `prds/DOC-master-catalog-prd-v1.0.md` - Tools not registered
- ⚠️ `prds/STD-data-architecture-impl-v1.0.md` - Outdated line counts (990 → 1,383)

## Updates Required

### 1. `prds/PRD-rvu-gpci-prd-v0.1.md` - Add Snapshot Registration
**File:** `prds/PRD-rvu-gpci-prd-v0.1.md`  
**Location:** After line 375 (after "Publish Stage Implementation")  
**Priority:** HIGH

**Content to add:**
```markdown
#### Snapshot Registration (Post-Publish)

**Location:** `RVUIngestor._register_dataset_snapshots` (lines 1104-1166)

After successful publish, snapshots are automatically registered in `dataset_snapshots` table:

**Datasets registered:**
- rvu_items, gpci_indices, anescf, localitycounty, oppscap

**Dataset-specific release IDs** (via `_dataset_release_id` helper):
| Dataset | Example Release ID |
|---------|-------------------|
| rvu_items | rvu_2025_B |
| gpci_indices | gpci_2025_B |
| anescf | anescf_2025_B |
| localitycounty | locality_2025_B |
| oppscap | oppscap_2025_B |

**Metadata stored:**
- SHA256 digest (Parquet file, computed in chunks)
- Effective dates (from vintage_date)
- Manifest URL (provenance)
- Curated path (actual Parquet location)

**Error handling:** Registration failures log warnings; pipeline continues

**Verification:**
bash
python tools/audit_snapshot_paths.py --dataset-id gpci_indices

**Tools:**
- `tools/audit_snapshot_paths.py` - Audit snapshot paths
- `scripts/repair_snapshot_paths.py` - Repair manifest.json paths
```

**Acceptance:** Section added with all metadata fields documented

### 2. `prds/RUN-global-operations-prd-v1.0.md` - Add Snapshot Verification
**File:** `prds/RUN-global-operations-prd-v1.0.md`  
**Location:** After section A.2 "Locality & GPCI Integrity" (around line 27)  
**Priority:** HIGH

**Content to add:**
```markdown
3) **RVU Snapshot Registration Verification**
- After RVU ingestion, verify dataset-specific release IDs registered:
bash
python tools/audit_snapshot_paths.py --show-all | grep -E "(rvu_items|gpci_indices)"

- Expected output: Each dataset shows its own prefix (`rvu_2025_B`, `gpci_2025_B`, etc.)
- If any show `status=manifest_json`, repair them:
bash
python scripts/repair_snapshot_paths.py --dataset-id gpci_indices --confirm

- Confirm MPFS dependency: MPFS ingestion requires `rvu_items` and `gpci_indices` snapshots with matching quarter suffixes
```

**Acceptance:** Verification commands added to operational checklist

### 3. `prds/DOC-master-catalog-prd-v1.0.md` - Register Operational Tools
**File:** `prds/DOC-master-catalog-prd-v1.0.md`  
**Location:** Section 8.1 after line 234 (after `audit_task_completion.py`)  
**Priority:** MEDIUM

**Content to add:**
```markdown
- `tools/audit_snapshot_paths.py` - Audits dataset_snapshots table for path resolution issues; flags manifest.json entries requiring repair
- `scripts/repair_snapshot_paths.py` - Repairs snapshot manifest_url fields pointing to .json files; updates to resolved parquet paths with CSV backup
```

**Acceptance:** Tools appear in catalog with descriptions

### 4. `prds/STD-data-architecture-impl-v1.0.md` - Update Line Counts
**File:** `prds/STD-data-architecture-impl-v1.0.md`  
**Locations:** Lines 2116, 2961, 3101  
**Priority:** MEDIUM

**Current (incorrect):**
- `<1,000 lines (RVU achieved 990 lines, 76.7% reduction from 4,247)`
- `After: 990 lines (76.7% reduction)`
- `(990 lines, completed migration)`

**Replace with (verified):**
- `<1,500 lines (RVU achieved 1,383 lines, 67.4% reduction from 4,247)`
- `After: 1,383 lines (67.4% reduction)`
- `(1,383 lines, completed migration)`

**Acceptance:** All 3 locations updated with accurate metrics

### 5. Create `prds/RUN-rvu-ingestion-v1.0.md` (Optional)
**File:** New file `prds/RUN-rvu-ingestion-v1.0.md`  
**Priority:** LOW (defer unless ops team requests)

**Minimum scope if created:**
1. **Pre-flight:** DB connectivity, file system check
2. **Command:** 
   ```bash
   python scripts/run_rvu_ingestion.py --year 2025 --quarter B
   ```
3. **Verification:**
   ```bash
   # Check curated outputs
   ls data/ingestion/rvu/curated/cms_rvu/*/
   
   # Verify snapshots
   python tools/audit_snapshot_paths.py --show-all
   ```
4. **Troubleshooting:** Common errors (missing source files, DB connection)

**Acceptance criteria:** Operator can run RVU ingestion following documented steps without eng support

**Decision:** Defer to separate issue unless requested

## Verification Commands (Specific to Files Touched)

After completing updates 1-4, run:
```bash
# Verify specific files touched
python tools/audit_doc_metadata.py --paths \
  prds/PRD-rvu-gpci-prd-v0.1.md \
  prds/RUN-global-operations-prd-v1.0.md \
  prds/DOC-master-catalog-prd-v1.0.md \
  prds/STD-data-architecture-impl-v1.0.md

# Check all cross-references still valid
python tools/audit_doc_links.py --paths \
  prds/PRD-rvu-gpci-prd-v0.1.md \
  prds/RUN-global-operations-prd-v1.0.md
```

## Summary
- **Total items:** 5
- **Already complete:** 2 (RUN-mpfs, PRD-mpfs)
- **Required:** 3 (PRD-rvu-gpci, RUN-global-ops, DOC-catalog)
- **Optional:** 1 (STD-architecture line counts)
- **Deferred:** 1 (RUN-rvu-ingestion runbook)


### To-dos

- [ ] Add snapshot registration section to PRD-rvu-gpci-prd-v0.1.md documenting automatic registration, dataset-specific release IDs, and verification commands
- [ ] Add RVU snapshot verification subsection to RUN-global-operations-prd-v1.0.md with audit and repair commands
- [ ] Register audit_snapshot_paths.py and repair_snapshot_paths.py in DOC-master-catalog-prd-v1.0.md tools section
- [ ] Update STD-data-architecture-impl-v1.0.md line count metrics in 3 locations (990→1,351 lines, 76.7%→68.2% reduction)
- [ ] Create RUN-rvu-ingestion-v1.0.md operational runbook using RUN-mpfs-ingestion-v1.0.md as template (optional)
- [ ] Run tools/audit_doc_metadata.py and tools/audit_doc_links.py to verify documentation consistency