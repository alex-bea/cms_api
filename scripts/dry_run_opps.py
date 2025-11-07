#!/usr/bin/env python3
"""
Dry-run harness for the OPPS ingestion pipeline.

Executes the OPPS 5-stage pipeline for a single batch ID and emits a concise
summary that operators can attach to readiness artifacts. Designed to mirror
the workflow documented in artifacts/ingestion_timeline_plan.md (Priority 0).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import structlog

from cms_pricing.ingestion.ingestors.opps_ingestor import OPPSIngestor

logger = structlog.get_logger()


def _build_evidence(
    batch_id: str,
    run_result: Dict[str, Any],
    output_dir: Path,
) -> Dict[str, Any]:
    """Shape a concise evidence payload from the ingestor response."""
    publish_results = run_result.get("publish_results") or {}
    curated_path = (output_dir / "curated" / "opps" / batch_id).resolve()
    metadata_path = curated_path / "metadata.json"
    curated_metadata: Optional[Dict[str, Any]] = None
    if metadata_path.exists():
        try:
            curated_metadata = json.loads(metadata_path.read_text())
        except json.JSONDecodeError:
            logger.warning("Unable to parse curated metadata", path=str(metadata_path))

    return {
        "batch_id": batch_id,
        "status": run_result.get("status", "unknown"),
        "timestamp": run_result.get("timestamp", datetime.utcnow().isoformat()),
        "stages_completed": run_result.get("stages_completed", []),
        "validation_passed": bool(
            (run_result.get("validation_results") or {}).get("passed", False)
        ),
        "tables_published": publish_results.get("tables_published", []),
        "records_published": publish_results.get("records_published", 0),
        "files_generated": publish_results.get("files_generated", []),
        "table_artifacts": publish_results.get("table_artifacts", []),
        "file_checksums": publish_results.get("file_checksums", {}),
        "curated_output_path": str(curated_path),
        "curated_exists": curated_path.exists(),
        "curated_metadata_path": str(metadata_path),
        "curated_metadata": curated_metadata,
    }


async def _run(args: argparse.Namespace) -> Dict[str, Any]:
    """Execute the OPPS ingestion pipeline for the provided batch ID."""
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.artifact_profile:
        os.environ["OPPS_ADDENDA_PROFILE"] = args.artifact_profile
        logger.info("Using explicit artifact profile override", profile=args.artifact_profile)
    elif _sandbox_samples_configured() and not os.getenv("OPPS_ADDENDA_PROFILE"):
        os.environ["OPPS_ADDENDA_PROFILE"] = "sandbox_addenda"
        logger.info("Auto-selected sandbox addenda profile for local samples", profile="sandbox_addenda")

    ingestor = OPPSIngestor(
        output_dir=output_dir,
        database_url=args.database_url,
        cpt_masking_enabled=not args.disable_cpt_masking,
    )

    logger.info(
        "Starting OPPS dry-run",
        batch_id=args.batch_id,
        output_dir=str(output_dir),
    )

    result = await ingestor.ingest_batch(args.batch_id)
    evidence = _build_evidence(args.batch_id, result, output_dir)

    if args.evidence_dir:
        evidence_dir = Path(args.evidence_dir).expanduser().resolve()
        evidence_dir.mkdir(parents=True, exist_ok=True)
        evidence_path = evidence_dir / f"{args.batch_id}_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
        evidence_path.write_text(json.dumps(evidence, indent=2))
        logger.info("Wrote dry-run evidence", path=str(evidence_path))

    return evidence


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the OPPS ingestion pipeline in dry-run mode and capture evidence."
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        help="Batch identifier to ingest (e.g., opps_2025q1_r01).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data"),
        help="Directory for raw/stage/curated artifacts (default: ./data).",
    )
    parser.add_argument(
        "--database-url",
        help="Optional database URL for publish stage verification.",
    )
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        default=Path("artifacts/opps_dry_runs"),
        help="Directory where JSON evidence files will be stored.",
    )
    parser.add_argument(
        "--disable-cpt-masking",
        action="store_true",
        help="Disable CPT masking when generating curated outputs.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print the evidence JSON to stdout.",
    )
    parser.add_argument(
        "--artifact-profile",
        help="Override OPPS addenda profile (sets OPPS_ADDENDA_PROFILE for this run).",
    )
    return parser.parse_args()


def _sandbox_samples_configured() -> bool:
    """Return True when local sample directories are configured via env vars."""
    return bool(
        os.getenv("OPPS_LOCAL_SAMPLE_DIR")
        or os.getenv("OPPS_LOCAL_SAMPLE_DIRS")
    )


def main() -> None:
    args = _parse_args()
    evidence = asyncio.run(_run(args))

    if args.pretty:
        print(json.dumps(evidence, indent=2))
    else:
        print(json.dumps(evidence))


if __name__ == "__main__":
    main()
