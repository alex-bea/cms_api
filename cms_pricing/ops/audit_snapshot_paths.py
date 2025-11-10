"""CLI utility for auditing dataset snapshot manifest paths."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable, Optional

from cms_pricing.database import SessionLocal
from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService


def resolve_snapshot_path(
    service: DatasetSnapshotService, snapshot: DatasetSnapshot
) -> Optional[Path]:
    """Use DatasetSnapshotService to resolve the curated path for a snapshot."""
    try:
        resolved = service._resolve_curated_path(snapshot)  # noqa: SLF001
        if resolved:
            return Path(resolved)
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"[WARN] Failed to resolve {snapshot.dataset_id}:{snapshot.release_id}: {exc}")
    return None


def audit_snapshots(
    dataset_id: str | None,
    limit: int,
    show_all: bool,
    session_factory: Optional[Callable[[], object]] = None,
) -> int:
    factory = session_factory or SessionLocal
    session = factory()
    service = DatasetSnapshotService(session)

    try:
        query = session.query(DatasetSnapshot)
        if dataset_id:
            query = query.filter(DatasetSnapshot.dataset_id == dataset_id)
        snapshots = (
            query.order_by(
                DatasetSnapshot.dataset_id,
                DatasetSnapshot.effective_from.desc(),
            )
            .limit(limit if limit > 0 else None)
            .all()
        )

        if not snapshots:
            print("No snapshots found for the given filters.")
            return 0

        issues = 0
        print(f"Inspecting {len(snapshots)} snapshot(s)...")

        for snap in snapshots:
            manifest = snap.manifest_url or "-"
            manifest_is_local = manifest.endswith(".json") and (
                manifest.startswith("data/") or manifest.startswith("./data/")
            )
            resolved_path = resolve_snapshot_path(service, snap)
            resolved_exists = resolved_path is not None and resolved_path.exists()

            status_parts: list[str] = []
            if manifest_is_local:
                status_parts.append("manifest_json")
            if resolved_path is None:
                status_parts.append("unresolved")
            elif not resolved_exists:
                status_parts.append("missing_target")
            else:
                status_parts.append("ok")

            status = ",".join(status_parts)
            if status != "ok" or show_all:
                issues += 0 if status == "ok" else 1
                print(
                    f"{snap.dataset_id:15} {snap.release_id:15} "
                    f"manifest={manifest} resolved={resolved_path or '-'} status={status}"
                )

        if issues:
            print(f"\nCompleted with {issues} issue(s) detected.")
            return 1

        print("\nAll inspected snapshots resolved to concrete parquet paths.")
        return 0
    finally:
        service.close()
        session.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit dataset snapshot manifest paths")
    parser.add_argument("--dataset-id", help="Dataset ID to filter (e.g., rvu_items)")
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum snapshots to inspect (default: 100)",
    )
    parser.add_argument(
        "--show-all",
        action="store_true",
        help="Print all snapshots, not just the ones with manifest/path issues",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return audit_snapshots(args.dataset_id, args.limit, args.show_all)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
