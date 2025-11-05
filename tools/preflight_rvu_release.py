"""
Preflight check before running RVU ingest.

Usage:
    python3 tools/preflight_rvu_release.py --source fast_2025

It will:
  • Fail if a release with the same source_version already exists.
  • Warn if the releases table already has more than N entries today.
  • Remind you to clear MAX_INGESTION_ROWS if set.
"""

from __future__ import annotations

import argparse
import os
from datetime import date

from sqlalchemy import text

from cms_pricing.database import SessionLocal


def main() -> None:
    parser = argparse.ArgumentParser(description="Preflight guard before RVU ingest.")
    parser.add_argument("--source", required=True, help="Expected source_version for the upcoming run (e.g., fast_2025Q4)")
    parser.add_argument("--max-today", type=int, default=3, help="Warn if more than this many RVU_FULL releases exist for today")
    args = parser.parse_args()

    session = SessionLocal()
    try:
        existing = session.execute(
            text("SELECT id, imported_at FROM releases WHERE type='RVU_FULL' AND source_version = :src"),
            {"src": args.source},
        ).fetchall()
        if existing:
            ids = ", ".join(row[0] for row in existing)
            raise SystemExit(f"❌ Release(s) already exist with source_version '{args.source}': {ids}")

        today = date.today()
        today_count = session.execute(
            text("SELECT COUNT(*) FROM releases WHERE type='RVU_FULL' AND imported_at = :today"),
            {"today": today},
        ).scalar()
        if today_count > args.max_today:
            print(f"⚠️ Warning: {today_count} RVU_FULL releases already exist for {today}.")

        if os.getenv("MAX_INGESTION_ROWS"):
            print(f"⚠️ MAX_INGESTION_ROWS is set to {os.getenv('MAX_INGESTION_ROWS')}. Remove it for full ingest runs.")

        print("✅ Preflight checks passed.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
