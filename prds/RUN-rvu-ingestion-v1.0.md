# Runbook: RVU Ingestion (v1.0)

**Document Type:** Operational Runbook  
**Status:** Draft v1.0 (2025-11-12)  
**Owners:** Data Engineering (RVU squad) + Platform Operations  
**Consumers:** Release engineers, On-call responders, Pricing Ops  
**Change control:** PR review + Ops sign-off

**Cross-References:**
- `prds/PRD-rvu-gpci-prd-v0.1.md` (product requirements)  
- `prds/STD-data-architecture-impl-v1.0.md` (ingestion patterns)  
- `prds/STD-database-platform-prd-v1.0.md` (snapshot guardrails)  
- `prds/RUN-mpfs-ingestion-v1.0.md` (downstream reuse runbook)  
- `prds/RUN-global-operations-prd-v1.0.md` (launch/ops playbook)  
- `prds/DOC-master-catalog-prd-v1.0.md` (master catalog entry)

---

## 1. Purpose & Scope
Run the DIS-compliant RVU ingestion pipeline that lands and publishes CMS artifacts:
- PPRRVU (RVU items)
- GPCI indices
- OPPS-based payment caps
- Anesthesia conversion factors
- Locality↔county mapping

Outputs power downstream MPFS, OPPS, and API workloads via dataset snapshots and curated parquet files.

---

## 2. Prerequisites & Access
- `DATABASE_URL` pointing at target Postgres environment (dev/staging/prod)
- Filesystem access to `${RVU_OUTPUT_DIR:-data/ingestion/rvu}` (≥5 GB free)
- Python virtualenv with project dependencies (`pip install -r requirements.txt`)
- Manifest retention: do **not** delete `manifest.json` files referenced by existing snapshots; repair tooling depends on them
- Tools/scripts available:
  - `python scripts/run_rvu_ingestion.py`
  - `python -m cms_pricing.ops.audit_snapshot_paths`
  - `python -m cms_pricing.ops.repair_snapshot_paths`
  - `python tools/check_snapshot_release_parity.py`
- Credentials for CMS downloads if running scraper discovery (default fixture data is local)

---

## 3. Pre-Flight Checklist
1. **Confirm release target** (quarter suffix or explicit release ID)
   ```bash
   python scripts/list_snapshots.py --dataset rvu_items | head
   ```
2. **Disk space**
   ```bash
   df -h data/ingestion/rvu
   ```
3. **Snapshot health (previous run)** – verify last snapshots resolve to parquet
   ```bash
   python -m cms_pricing.ops.audit_snapshot_paths --dataset-id gpci_indices --show-all
   ```
4. **Parity check** (optional but recommended before quarterly runs)
   ```bash
   python tools/check_snapshot_release_parity.py --pairs rvu_items:gpci_indices
   ```
5. **Environment vars** – ensure `DATABASE_URL`, `RVU_OUTPUT_DIR`, proxy settings (if required) are exported

Do not proceed until audit reports `status=ok` or you have repaired manifest paths.

---

## 4. Execution Steps
### 4.1 Command
```bash
export RVU_OUTPUT_DIR=${RVU_OUTPUT_DIR:-data/ingestion/rvu}
python scripts/run_rvu_ingestion.py \
  --release-id rvu_2025_B \
  --output-dir "$RVU_OUTPUT_DIR"
```
- `--release-id` accepts CMS suffixes (e.g., `rvu_2025_B`, `rvu_2025_DR`). When omitted, latest release is used.
- `--output-dir` defaults to `data/ingestion/rvu`; override for prod paths.
- Additional flags: `--log-level`, `--scraper-cache-dir`, `--resume-from` (rarely needed).

### 4.2 Log Monitoring
```bash
tail -f "$RVU_OUTPUT_DIR"/logs/rvu_ingestion_*.log
```
Key checkpoints:
- `Starting RVU land stage ... files_downloaded=X`
- `Validation summary ... issues=0`
- `Publish stage completed ... curated_tables=5`
- `Registered dataset snapshot ... dataset_id=<name> release_id=<prefix_year_suffix>`

Pipeline typically completes in 15–30 minutes depending on download speed.

---

## 5. Post-Run Verification
1. **Manifest + curated outputs**
   ```bash
   MANIFEST=$(find "$RVU_OUTPUT_DIR"/curated -name manifest.json | sort | tail -1)
   jq 'keys' "$MANIFEST"
   ```
   Expect entries for `pprrvu`, `gpci`, `anescf`, `localitycounty`, `oppscap`.
2. **Snapshot audit (mandatory)**
   ```bash
   python -m cms_pricing.ops.audit_snapshot_paths --dataset-id gpci_indices --show-all
   ```
   Ensure each dataset reports `status=ok` with release IDs like `gpci_2025_B`.
3. **Repair if needed**
   ```bash
   export SNAPSHOT_SEARCH_ROOTS=/var/data/ingestion/production/curated
   python -m cms_pricing.ops.repair_snapshot_paths --dataset-id gpci_indices --confirm --backup /tmp/gpci_snapshot_backup.csv --use-latest-drop
   ```
   Attach the CSV you generated (e.g., `/tmp/gpci_snapshot_backup.csv`) to the ops ticket.

> **Shortcut:** `./scripts/render_snapshot_postdeploy.sh` packages the audit → repair → audit → preflight flow on Render so you can attach the resulting evidence directory to the change ticket.
4. **Row count sanity checks**
   ```bash
   python - <<'PY'
   import pandas as pd
   from pathlib import Path
   m = Path("$MANIFEST")
   for name in ("pprrvu", "gpci", "localitycounty"):
       path = m.parent / f"{name}.parquet"
       if path.exists():
           df = pd.read_parquet(path)
           print(name, len(df))
   PY
   ```
5. **Registering release IDs** – optional SQL check
   ```bash
   psql "$DATABASE_URL" -c "SELECT dataset_id, release_id, created_at FROM dataset_snapshots ORDER BY created_at DESC LIMIT 10;"
   ```

Record manifest path, release_id, batch_id, and audit output.

---

## 6. Troubleshooting
| Symptom | Likely Cause | Resolution |
|---------|--------------|------------|
| `SchemaOutOfDateError: Database schema revision ...` | Postgres migrations behind Alembic head | Run `alembic upgrade head`, rerun preflight, and restart ingestion |
| `ValueError: RVU snapshot not available` | Wrong release ID or snapshots missing | Run latest release without `--release-id` or ingest missing quarter first |
| `manifest_path` still `.json` after run | File moved or deleted | Run repair script; ensure manifests are retained in backup |
| Parser errors on download files | CMS changed schema or corrupted download | Inspect raw files under `$RVU_OUTPUT_DIR/raw`; update parser or re-download |
| Publish stage warning `Snapshot service unavailable` | DB session issue | Restart ingestion with valid `DATABASE_URL`; check DB connectivity |
| Validation failures (issues>0) | Schema drift or bad data | Review `logs/validation/*.log`, escalate to data engineering |

Escalation: if ingestion fails twice or data discrepancies persist, page Data Engineering (RVU squad Slack channel) and file incident ticket.

---

## 7. Run Artifact Checklist
- Run timestamp, operator, command (release_id + output_dir)
- Manifest path + curated row counts
- `audit_snapshot_paths.py` output (attach text)
- Repair CSV (if applicable)
- Log snippet showing `Registered dataset snapshot ...`
- Notes on validation warnings or follow-up tasks

Store in Ops ticket or `artifacts/rvu_run_log.md` if using shared ledger.

---

## 8. Supporting Tools
- `cms_pricing.ops.audit_snapshot_paths` – verifies snapshot metadata resolves to parquet
- `cms_pricing.ops.repair_snapshot_paths` – rewrites legacy manifest URLs (requires `--confirm`)
- `tools/check_snapshot_release_parity.py` – ensures RVU/GPCI suffixes match (runs in CI)
- `tools/verify_source_map.py` – validates discovery manifests vs reference source map

---

## 9. Change Log
| Version | Date | Summary |
|---------|------|---------|
| 1.0 | 2025-11-12 | Initial RVU ingestion runbook covering CLI workflow, verification, and snapshot tooling |
