"""
Utility script to wipe RVU release data by source_version or release_id.

Usage examples (Render shell):
    # Remove the latest RVU_FULL release
    python3 tools/reset_rvu_release.py --latest

    # Remove all releases whose source_version matches "fast_2025"
    python3 tools/reset_rvu_release.py --source fast_2025

    # Remove a specific release by UUID
    python3 tools/reset_rvu_release.py --release-id 123e4567-e89b-12d3-a456-426614174000
"""

from __future__ import annotations

import argparse
from typing import Iterable

from sqlalchemy import text

from cms_pricing.database import SessionLocal


TABLES = ["rvu_items", "gpci_indices", "opps_caps", "anes_cfs", "locality_counties"]


def delete_release(session, release_id: str) -> None:
    for table in TABLES:
        session.execute(text(f"DELETE FROM {table} WHERE release_id = :rid"), {"rid": release_id})
    session.execute(text("DELETE FROM releases WHERE id = :rid"), {"rid": release_id})


def resolve_releases(session, args: argparse.Namespace) -> Iterable[str]:
    if args.latest:
        row = session.execute(
            text("SELECT id FROM releases WHERE type='RVU_FULL' ORDER BY imported_at DESC LIMIT 1")
        ).fetchone()
        if row:
            return [row[0]]
        return []

    if args.release_id:
        return [args.release_id]

    if args.source:
        rows = session.execute(
            text("SELECT id FROM releases WHERE type='RVU_FULL' AND source_version LIKE :src"),
            {"src": f"%{args.source}%"},
        ).fetchall()
        return [row[0] for row in rows]

    raise ValueError("No selection provided. Use --latest, --release-id, or --source.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete RVU releases and child rows.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--latest", action="store_true", help="Delete the most recent RVU_FULL release")
    group.add_argument("--release-id", help="Delete a specific release UUID")
    group.add_argument("--source", help="Delete all RVU_FULL releases whose source_version matches this string")
    parser.add_argument("--dry-run", action="store_true", help="Show releases that would be deleted without applying changes")
    args = parser.parse_args()

    session = SessionLocal()
    try:
        release_ids = list(resolve_releases(session, args))
        if not release_ids:
            print("No matching releases found.")
            return

        print("Selected release IDs:")
        for rid in release_ids:
            print(f"  {rid}")

        if args.dry_run:
            print("Dry run mode - no changes applied.")
            return

        for rid in release_ids:
            delete_release(session, rid)
        session.commit()
        print(f"Deleted {len(release_ids)} release(s).")
    finally:
        session.close()


if __name__ == "__main__":
    main()
