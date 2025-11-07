#!/usr/bin/env python3
"""Check that paired dataset snapshots share the same release suffix.

Examples:
    python tools/check_snapshot_release_parity.py --pairs rvu_items:gpci_indices
    python tools/check_snapshot_release_parity.py --allow-missing

Intended for daily/CI monitoring to ensure RVU→GPCI parity before MPFS runs.
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Tuple, Optional

from pathlib import Path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from cms_pricing.database import SessionLocal
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService


def extract_suffix(release_id: Optional[str]) -> Optional[str]:
    if not release_id:
        return None
    release_id = release_id.strip()
    if not release_id:
        return None
    parts = [token for token in release_id.split("_") if token]
    if len(parts) >= 3:
        return parts[-1]
    return None


def check_pair(service: DatasetSnapshotService, primary: str, secondary: str, allow_missing: bool) -> Tuple[bool, str]:
    primary_snap = service.get_latest_snapshot(primary)
    secondary_snap = service.get_latest_snapshot(secondary)

    if not primary_snap or not secondary_snap:
        if allow_missing:
            return True, f"Skipped pair {primary}:{secondary} (missing snapshot)."
        missing = []
        if not primary_snap:
            missing.append(primary)
        if not secondary_snap:
            missing.append(secondary)
        return False, f"Missing snapshot(s) for pair {primary}:{secondary}: {', '.join(missing)}"

    primary_suffix = extract_suffix(primary_snap.release_id)
    secondary_suffix = extract_suffix(secondary_snap.release_id)

    if primary_suffix != secondary_suffix:
        return (
            False,
            f"Release mismatch {primary_snap.release_id} vs {secondary_snap.release_id}",
        )

    return True, (
        f"Pair {primary}:{secondary} OK -> {primary_snap.release_id} / {secondary_snap.release_id}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Ensure paired dataset snapshots use matching release suffixes")
    parser.add_argument(
        "--pairs",
        action="append",
        default=["rvu_items:gpci_indices"],
        help="Dataset pairs to compare (format primary:secondary). Can be specified multiple times.",
    )
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Do not fail when a dataset is missing (prints warning instead).",
    )

    args = parser.parse_args()

    session = SessionLocal()
    service = DatasetSnapshotService(session)

    pairs: List[Tuple[str, str]] = []
    for pair_str in args.pairs:
        if ":" not in pair_str:
            parser.error(f"Invalid pair format: {pair_str}")
        left, right = pair_str.split(":", 1)
        pairs.append((left.strip(), right.strip()))

    try:
        failures = 0
        for primary, secondary in pairs:
            ok, message = check_pair(service, primary, secondary, args.allow_missing)
            if ok:
                print(f"[OK] {message}")
            else:
                print(f"[FAIL] {message}")
                failures += 1
        if failures:
            return 1
        print("All dataset pairs share matching release suffixes.")
        return 0
    finally:
        service.close()
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
