#!/usr/bin/env python3
"""
Run the RVU ingestion pipeline against real CMS drops and verify row counts.

Usage examples:
    # Run ingestion and then print table counts
    python scripts/rvu_ingest_audit.py --database-url postgresql://user:pass@host/db

    # Skip ingestion (counts only)
    python scripts/rvu_ingest_audit.py --skip-ingest

Optional flags let you override the output directory, release_id, and batch_id
so the run metadata matches your operations log.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import datetime, date
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

# Ensure project root on sys.path when script executed directly
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor  # noqa: E402


DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "ingestion" / "rvu_production"

# Tables to audit after ingestion (table_name -> human readable description)
AUDIT_TABLES = {
    "rvu_items": "PPRRVU items (rvu_items)",
    "gpci_indices": "GPCI indices (gpci_indices)",
    "opps_caps": "OPPS payment caps (opps_caps)",
    "anes_cfs": "Anesthesia conversion factors (anes_cfs)",
    "locality_counties": "Locality to county mapping (locality_counties)",
    "dataset_snapshots": "Dataset snapshot registry (dataset_snapshots)",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        dest="database_url",
        default=os.getenv("DATABASE_URL"),
        help="Database connection string (overrides DATABASE_URL env var).",
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Directory for ingestion artifacts (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--release-id",
        dest="release_id",
        help="Override release_id for this ingestion run (default: rvu_YYYYMMDD).",
    )
    parser.add_argument(
        "--batch-id",
        dest="batch_id",
        help="Override batch_id for this ingestion run (default: batch_<epoch>).",
    )
    parser.add_argument(
        "--skip-ingest",
        dest="skip_ingest",
        action="store_true",
        help="Skip ingestion and only print table counts.",
    )
    parser.add_argument(
        "--expect-nonzero",
        dest="expect_nonzero",
        action="store_true",
        help="Exit with code 1 if any audited table has zero rows.",
    )
    return parser.parse_args()


def resolve_ids(args: argparse.Namespace) -> tuple[str, str]:
    release_id = args.release_id or f"rvu_{date.today():%Y%m%d}"
    batch_id = args.batch_id or f"batch_{int(time.time())}"
    return release_id, batch_id


async def run_ingestion(output_dir: Path, session: Session, release_id: str, batch_id: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    ingestor = RVUIngestor(output_dir=str(output_dir), db_session=session)
    result = await ingestor.ingest(release_id=release_id, batch_id=batch_id)
    status = result.get("status", "unknown")
    if status != "success":
        raise SystemExit(f"RVU ingestion failed (status={status}): {result.get('error')}")


def print_table_counts(engine):
    print("\n=== RVU Table Counts ===")
    counts = {}
    with engine.begin() as connection:
        for table, description in AUDIT_TABLES.items():
            try:
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar_one()
            except Exception as exc:  # pragma: no cover - defensive
                print(f"{description}: ERROR ({exc})")
                counts[table] = None
                continue
            print(f"{description}: {count:,}")
            counts[table] = count
    print("========================\n")
    return counts


def main() -> None:
    args = parse_args()

    if not args.database_url:
        raise SystemExit("Database URL not provided. Use --database-url or set DATABASE_URL.")

    engine = create_engine(args.database_url)
    SessionFactory = sessionmaker(bind=engine)

    release_id, batch_id = resolve_ids(args)
    output_dir = Path(args.output_dir)

    if args.skip_ingest:
        print("Skipping ingestion (counts only).")
    else:
        print(f"Starting RVU ingestion (release_id={release_id}, batch_id={batch_id})...")
        session = SessionFactory()
        try:
            asyncio.run(run_ingestion(output_dir, session, release_id, batch_id))
            session.commit()
            print("Ingestion completed successfully.")
        finally:
            session.close()

    counts = print_table_counts(engine)

    if args.expect_nonzero:
        zero_tables = [desc for table, desc in AUDIT_TABLES.items() if counts.get(table, 0) == 0]
        if zero_tables:
            names = ", ".join(zero_tables)
            raise SystemExit(f"ERROR: Expected non-zero counts for {names}")


if __name__ == "__main__":
    main()
