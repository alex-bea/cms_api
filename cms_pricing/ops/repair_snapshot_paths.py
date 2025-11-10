"""CLI utility for repairing dataset snapshot manifest paths."""

from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from cms_pricing.database import SessionLocal
from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService


def resolve_new_path(service: DatasetSnapshotService, snapshot: DatasetSnapshot) -> Optional[str]:
    """Use DatasetSnapshotService to resolve a parquet path for snapshot."""
    try:
        return service._resolve_curated_path(snapshot)  # noqa: SLF001
    except Exception as exc:  # pragma: no cover - defensive logging
        print(
            f"[WARN] Failed to resolve {snapshot.dataset_id}:{snapshot.release_id}: {exc}",
        )
        return None


def audit_and_repair(
    dataset_id: Optional[str],
    release_id: Optional[str],
    limit: Optional[int],
    dry_run: bool,
    confirm: bool,
    backup_path: Path,
    search_roots: Optional[Sequence[Path]] = None,
) -> int:
    session = SessionLocal()
    service = DatasetSnapshotService(session)
    candidate_roots = _collect_search_roots(search_roots)

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

        repairs: List[Tuple[DatasetSnapshot, str]] = []
        for snap in snapshots:
            new_path = resolve_new_path(service, snap)
            if not new_path:
                print(
                    f"[WARN] Could not resolve parquet path for {snap.dataset_id}:{snap.release_id}",
                )
                continue

            normalized = DatasetSnapshotService._normalize_ingestion_path(Path(new_path))
            normalized = DatasetSnapshotService._dedupe_repeated_prefix(normalized)

            filesystem_path = _resolve_filesystem_path(normalized, candidate_roots)
            if not filesystem_path:
                print(
                    f"[WARN] Skipping {snap.dataset_id}:{snap.release_id} — resolved parquet missing at {normalized.as_posix()}",
                )
                continue

            normalized_str = normalized.as_posix()

            if normalized_str == (snap.manifest_url or "").strip():
                continue
            repairs.append((snap, normalized_str))

        if not repairs:
            print("No snapshot entries required repair.")
            return 0

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
                f"Dry run complete ({len(repairs)} rows). Use --confirm without --dry-run to apply changes."
            )
            return 0

        for snap, new_path in repairs:
            snap.manifest_url = new_path
        session.commit()
        print(f"Updated {len(repairs)} snapshot(s).")
        return 0
    finally:
        service.close()
        session.close()


def build_parser() -> argparse.ArgumentParser:
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
    parser.add_argument(
        "--search-root",
        action="append",
        type=Path,
        help="Additional filesystem roots to search when manifest paths are repo-relative (can be provided multiple times).",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
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
        search_roots=args.search_root,
    )


def _collect_search_roots(explicit_roots: Optional[Sequence[Path]]) -> List[Path]:
    """Build ordered list of filesystem roots used to locate parquet files."""
    roots: List[Path] = []

    def _add(root: Path) -> None:
        root = Path(root)
        if root not in roots:
            roots.append(root)

    _add(Path.cwd())

    env_roots = os.getenv("SNAPSHOT_SEARCH_ROOTS")
    if env_roots:
        for chunk in env_roots.split(os.pathsep):
            chunk = chunk.strip()
            if chunk:
                _add(Path(chunk))

    _add(Path("/var"))

    if explicit_roots:
        for root in explicit_roots:
            _add(root)

    return roots


def _resolve_filesystem_path(normalized: Path, search_roots: Sequence[Path]) -> Optional[Path]:
    """Return the first filesystem path that exists for the provided normalized path."""
    if normalized.is_absolute():
        return normalized if normalized.exists() else None

    for base in search_roots:
        candidate = Path(base) / normalized
        if candidate.exists():
            return candidate

    return None


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
