#!/usr/bin/env python3
"""Repair dataset_snapshots records whose manifest path points to manifest.json.

Usage:
    python scripts/repair_snapshot_paths.py --dataset-id gpci_indices --confirm

Key behaviors:
- Scans dataset_snapshots (optionally filtered by dataset and/or release)
- For rows whose manifest_url is a local manifest JSON, resolves the parquet
  path using the manifest contents (same logic as DatasetSnapshotService)
- Updates manifest_url to the resolved parquet path (unless --dry-run)
- Writes a CSV backup of modifications for rollback
- Refuses to mutate data unless --confirm flag is supplied
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from cms_pricing.database import SessionLocal
from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService


def resolve_new_path(service: DatasetSnapshotService, snapshot: DatasetSnapshot) -> Optional[str]:
    """Use DatasetSnapshotService to resolve a parquet path for snapshot."""
    try:
        resolved = service._resolve_curated_path(snapshot)  # pylint: disable=protected-access
    except Exception as exc:  # pragma: no cover - defensive logging only
        print(
            f"[WARN] Failed to resolve {snapshot.dataset_id}:{snapshot.release_id}: {exc}",
            file=sys.stderr,
        )
        return None
    return resolved


def should_repair(manifest_url: Optional[str]) -> bool:
    if not manifest_url:
        return False
    manifest_url = manifest_url.strip()
    if not manifest_url:
        return False
    return (
        manifest_url.endswith(".json")
        and (manifest_url.startswith("data/") or manifest_url.startswith("./data/"))
    )


def audit_and_repair(
    dataset_id: Optional[str],
    release_id: Optional[str],
    limit: Optional[int],
    dry_run: bool,
    confirm: bool,
    backup_path: Path,
) -> int:
    session = SessionLocal()
    service = DatasetSnapshotService(session)

    try:
        query = session.query(DatasetSnapshot)
        if dataset_id:
            query = query.filter(DatasetSnapshot.dataset_id == dataset_id)
        if release_id:
            query = query.filter(DatasetSnapshot.release_id == release_id)
        query = query.order_by(
            DatasetSnapshot.dataset_id,
            DatasetSnapshot.release_id,
        )
        if limit:
            query = query.limit(limit)

        snapshots: List[DatasetSnapshot] = query.all()
        if not snapshots:
            print("No snapshots matched the provided filters.")
            return 0

        repairs = []
        for snap in snapshots:
            if not should_repair(snap.manifest_url):
                continue
            new_path = resolve_new_path(service, snap)
            if not new_path:
                print(
                    f"[WARN] Could not resolve parquet path for {snap.dataset_id}:{snap.release_id}",
                    file=sys.stderr,
                )
                continue
            if new_path == snap.manifest_url:
                continue
            repairs.append((snap, new_path))

        if not repairs:
            print("No manifest.json entries required repair.")
            return 0

        # Write backup CSV regardless of dry run
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        with backup_path.open("w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                [
                    "dataset_id",
                    "release_id",
                    "old_manifest_url",
                    "new_path",
                    "effective_from",
                    "effective_to",
                ]
            )
            for snap, new_path in repairs:
                writer.writerow(
                    [
                        snap.dataset_id,
                        snap.release_id,
                        snap.manifest_url,
                        new_path,
                        snap.effective_from,
                        snap.effective_to,
                    ]
                )
        print(f"Backup written to {backup_path}")

        if dry_run or not confirm:
            print(
                f"Dry run complete ({len(repairs)} rows). Use --confirm without --dry-run to apply changes.")
            return 0

        for snap, new_path in repairs:
            snap.manifest_url = new_path
        session.commit()
        print(f"Updated {len(repairs)} snapshot(s).")
        return 0
    finally:
        service.close()
        session.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair dataset snapshot manifest paths")
    parser.add_argument("--dataset-id", help="Filter by dataset ID (e.g., gpci_indices)")
    parser.add_argument("--release-id", help="Filter by release ID (e.g., gpci_2025_B)")
    parser.add_argument("--limit", type=int, help="Limit number of rows scanned")
    parser.add_argument("--dry-run", action="store_true", help="Scan and log without modifying data")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required to apply changes. Without this flag the script only reports findings.",
    )
    parser.add_argument(
        "--backup",
        type=Path,
        help="Optional path for CSV backup (default: artifacts/snapshot_repairs/<timestamp>.csv)",
    )

    args = parser.parse_args()
    dry_run = args.dry_run or not args.confirm
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_path = args.backup or Path("artifacts/snapshot_repairs") / f"snapshot_repair_{timestamp}.csv"

    return audit_and_repair(
        dataset_id=args.dataset_id,
        release_id=args.release_id,
        limit=args.limit,
        dry_run=dry_run,
        confirm=args.confirm,
        backup_path=backup_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
