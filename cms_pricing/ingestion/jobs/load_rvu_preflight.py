"""CLI entrypoint for RVU preflight reseed runs."""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from pathlib import Path

from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor

logger = logging.getLogger(__name__)


def run_preflight(release_id: str, output_dir: str) -> int:
    """Run RVUIngestor in file-only mode to reseed curated parquet outputs."""
    target_dir = Path(output_dir or "data/ingestion/rvu")
    target_dir.mkdir(parents=True, exist_ok=True)

    batch_id = f"preflight_{int(time.time())}"
    logger.info(
        "Starting RVU preflight ingestion",
        extra={"release_id": release_id, "output_dir": str(target_dir), "batch_id": batch_id},
    )

    ingestor = RVUIngestor(
        output_dir=str(target_dir),
        db_session=None,
        enable_snapshot_registration=False,
    )
    result = asyncio.run(ingestor.ingest(release_id=release_id, batch_id=batch_id))
    status = result.get("status", "unknown")
    total_records = result.get("total_records", 0)

    logger.info("Preflight completed: status=%s total_records=%s", status, total_records)
    return 0 if status == "success" else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run RVU preflight reseed (parquet only)")
    parser.add_argument(
        "--release-id",
        default="rvu_2025_prod",
        help="RVU release identifier (default: rvu_2025_prod)",
    )
    parser.add_argument(
        "--output-dir",
        default="data/ingestion/rvu",
        help="Destination for curated parquet outputs (default: data/ingestion/rvu)",
    )
    return parser


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = build_parser().parse_args()
    return run_preflight(args.release_id, args.output_dir)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
