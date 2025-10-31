#!/usr/bin/env python3
"""Register dataset snapshots from ingestion metadata

This script reads release_id and batch_id from fee schedule tables and
registers corresponding entries in dataset_snapshots.

Usage:
    python scripts/register_dataset_snapshots.py --dataset MPFS
    python scripts/register_dataset_snapshots.py --all
"""

import argparse
import sys
import hashlib
from datetime import date
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.orm import Session
from sqlalchemy import func
import structlog

from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import (
    FeeMPFS, FeeOPPS, FeeASC, FeeCLFS, FeeDMEPOS, FeeIPPS
)
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService

logger = structlog.get_logger()

# Map dataset IDs to their models
DATASET_MODELS = {
    "MPFS": FeeMPFS,
    "OPPS": FeeOPPS,
    "ASC": FeeASC,
    "CLFS": FeeCLFS,
    "DMEPOS": FeeDMEPOS,
    "IPPS": FeeIPPS,
}


def extract_unique_releases(db: Session, model_class, dataset_id: str) -> list[dict]:
    """Extract unique release_id values from fee schedule table"""
    releases = db.query(
        model_class.release_id,
        func.min(model_class.effective_from).label('min_effective_from'),
        func.max(model_class.effective_to).label('max_effective_to')
    ).filter(
        model_class.release_id.isnot(None)
    ).group_by(model_class.release_id).all()
    
    return [
        {
            "release_id": r.release_id,
            "effective_from": r.min_effective_from,
            "effective_to": r.max_effective_to if r.max_effective_to else None  # Use actual effective_to, fallback to None
        }
        for r in releases
        if r.release_id
    ]


def register_dataset_snapshots(
    db: Session,
    dataset_id: str,
    digest: str = None,
    manifest_url: str = None
):
    """Register snapshots for a dataset from fee schedule tables"""
    
    if dataset_id not in DATASET_MODELS:
        logger.error("Unknown dataset", dataset_id=dataset_id)
        return
    
    model_class = DATASET_MODELS[dataset_id]
    snapshot_service = DatasetSnapshotService(db)
    
    # Extract unique releases
    releases = extract_unique_releases(db, model_class, dataset_id)
    
    if not releases:
        logger.warning("No releases found", dataset_id=dataset_id)
        return
    
    logger.info("Found releases", dataset_id=dataset_id, count=len(releases))
    
    # Batch register snapshots in a single transaction for efficiency
    from cms_pricing.models.dataset_snapshots import DatasetSnapshot
    
    registered = 0
    updated = 0
    
    for release in releases:
        # Generate digest if not provided (compute actual SHA256 hash of release_id)
        if digest:
            release_digest = digest
        else:
            # Compute actual SHA256 hash of release_id until we can pull from ingestion metadata
            release_digest = hashlib.sha256(release['release_id'].encode()).hexdigest()
        
        # Check if exists (efficient PK lookup)
        existing = db.get(DatasetSnapshot, (dataset_id, release['release_id']))
        
        if existing:
            # Update existing snapshot
            existing.digest = release_digest
            existing.effective_from = release['effective_from'] or date.today()
            existing.effective_to = release['effective_to']
            existing.manifest_url = manifest_url
            updated += 1
        else:
            # Create new snapshot
            snapshot = DatasetSnapshot(
                dataset_id=dataset_id,
                release_id=release['release_id'],
                digest=release_digest,
                effective_from=release['effective_from'] or date.today(),
                effective_to=release['effective_to'],
                manifest_url=manifest_url
            )
            db.add(snapshot)
            registered += 1
    
    # Commit all changes in a single transaction
    db.commit()
    
    logger.info(
        "Registration complete",
        dataset_id=dataset_id,
        registered=registered,
        updated=updated
    )


def main():
    parser = argparse.ArgumentParser(description="Register dataset snapshots")
    parser.add_argument(
        "--dataset",
        choices=list(DATASET_MODELS.keys()) + ["ALL"],
        help="Dataset to register (or ALL)"
    )
    parser.add_argument(
        "--digest",
        help="SHA256 digest to use (default: auto-generated)"
    )
    parser.add_argument(
        "--manifest-url",
        help="Manifest URL for snapshot"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Register snapshots for all datasets"
    )
    
    args = parser.parse_args()
    
    if not args.dataset and not args.all:
        parser.error("Must specify --dataset or --all")
    
    db = SessionLocal()
    try:
        if args.all or args.dataset == "ALL":
            for dataset_id in DATASET_MODELS.keys():
                register_dataset_snapshots(
                    db,
                    dataset_id,
                    args.digest,
                    args.manifest_url
                )
        else:
            register_dataset_snapshots(
                db,
                args.dataset,
                args.digest,
                args.manifest_url
            )
    finally:
        db.close()


if __name__ == "__main__":
    main()

