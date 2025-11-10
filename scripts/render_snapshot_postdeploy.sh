#!/usr/bin/env bash

# Run the standard Render snapshot audit → repair → preflight sequence
# and save every artifact (audit logs, CSV backups, preflight log) locally
# so they can be attached to change tickets.

set -euo pipefail

DATASETS=("rvu_items" "gpci_indices")

TIMESTAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
OUTPUT_DIR="${1:-artifacts/render_snapshot_evidence/$TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"

export SNAPSHOT_SEARCH_ROOTS="${SNAPSHOT_SEARCH_ROOTS:-/var/data/ingestion/production/curated}"

echo "[INFO] Writing evidence to $OUTPUT_DIR"

function audit_phase() {
  local phase="$1"
  for dataset in "${DATASETS[@]}"; do
    log_file="$OUTPUT_DIR/audit_${dataset}_${phase}.log"
    echo "[INFO] Auditing $dataset ($phase) → $log_file"
    python -m cms_pricing.ops.audit_snapshot_paths \
      --dataset-id "$dataset" \
      --show-all | tee "$log_file"
  done
}

# Phase 1: Pre-repair audit
audit_phase "before"

# Phase 2: Repair each dataset (writes CSV backups into OUTPUT_DIR)
for dataset in "${DATASETS[@]}"; do
  backup="$OUTPUT_DIR/${dataset}_snapshot_backup.csv"
  echo "[INFO] Repairing $dataset → $backup"
  python -m cms_pricing.ops.repair_snapshot_paths \
    --dataset-id "$dataset" \
    --confirm \
    --use-latest-drop \
    --backup "$backup"
done

# Phase 3: Post-repair audit
audit_phase "after"

# Phase 4: Preflight (single log file)
PREFLIGHT_LOG="$OUTPUT_DIR/preflight_snapshot_preflight.log"
echo "[INFO] Running preflight → $PREFLIGHT_LOG"
python -m cms_pricing.ingestion.ops.preflight \
  --log-path "$PREFLIGHT_LOG" \
  $(printf -- "--dataset-id %s " "${DATASETS[@]}")

echo "[INFO] Evidence ready in $OUTPUT_DIR"
