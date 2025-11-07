#!/usr/bin/env bash
# Helper to run the OPPS dry-run using local Section 508 samples.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BATCH_ID="${1:-opps_2025q1_r01}"

DEFAULT_SAMPLE_A="$ROOT_DIR/sample_data/january_202025_20web_20addendum_20a.12.31.24"
DEFAULT_SAMPLE_B="$ROOT_DIR/sample_data/january_2025_web_addendum_b.12.31.24"

export OPPS_LOCAL_SAMPLE_DIR="${OPPS_LOCAL_SAMPLE_DIR:-$DEFAULT_SAMPLE_A}"
if [[ ! -d "$OPPS_LOCAL_SAMPLE_DIR" ]]; then
  echo "ERROR: OPPS_LOCAL_SAMPLE_DIR does not exist: $OPPS_LOCAL_SAMPLE_DIR" >&2
  exit 1
fi

SAMPLE_B_DIR="${OPPS_LOCAL_SAMPLE_DIRS_OVERRIDE:-$DEFAULT_SAMPLE_B}"
if [[ -d "$SAMPLE_B_DIR" ]]; then
  export OPPS_LOCAL_SAMPLE_DIRS="${OPPS_LOCAL_SAMPLE_DIRS:-${OPPS_LOCAL_SAMPLE_DIR}:$SAMPLE_B_DIR}"
else
  export OPPS_LOCAL_SAMPLE_DIRS="${OPPS_LOCAL_SAMPLE_DIRS:-$OPPS_LOCAL_SAMPLE_DIR}"
fi

export OPPS_ADDENDA_PROFILE="${OPPS_ADDENDA_PROFILE:-sandbox_addenda}"

OUTPUT_DIR="${OPPS_OUTPUT_DIR:-$ROOT_DIR/data}"
EVIDENCE_DIR="${OPPS_EVIDENCE_DIR:-$ROOT_DIR/artifacts/opps_dry_runs}"

echo "Running OPPS sandbox dry-run..."
echo "  Batch ID:        $BATCH_ID"
echo "  Sample A (Add A): $OPPS_LOCAL_SAMPLE_DIR"
echo "  Sample B (Add B): ${SAMPLE_B_DIR:-'(not provided)'}"
echo "  Artifact profile: $OPPS_ADDENDA_PROFILE"

"$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/dry_run_opps.py" \
  --batch-id "$BATCH_ID" \
  --output-dir "$OUTPUT_DIR" \
  --evidence-dir "$EVIDENCE_DIR" \
  --pretty
