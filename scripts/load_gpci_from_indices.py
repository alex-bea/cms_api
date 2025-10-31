#!/usr/bin/env python3
"""
Load GPCI data from gpci_indices (RVU model) to gpci (fee schedule table).

This script:
1. Reads from gpci_indices table (where RVU ingestor loads data)
2. Transforms to simplified gpci table schema
3. Adds provenance metadata (release_id, batch_id)
4. Loads to gpci table for use by pricing engines
"""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import GPCIIndex
from cms_pricing.models.fee_schedules import GPCI
import structlog

logger = structlog.get_logger()


def load_gpci_from_indices(release_id: str = None, batch_id: str = None):
    """
    Load GPCI data from gpci_indices to gpci table.
    
    Args:
        release_id: Release identifier (defaults to 'rvu_gpci_<year>')
        batch_id: Batch identifier (defaults to timestamp-based)
    """
    db = SessionLocal()
    
    try:
        # Get all GPCI indices
        logger.info("Fetching GPCI data from gpci_indices table...")
        gpci_indices = db.query(GPCIIndex).all()
        
        if not gpci_indices:
            logger.warning("No data found in gpci_indices table")
            return 0
        
        logger.info(f"Found {len(gpci_indices)} rows in gpci_indices")
        
        # Determine release_id and batch_id if not provided
        if not release_id:
            # Use the release from the first record
            if gpci_indices[0].release:
                release_id = f"rvu_{gpci_indices[0].release.source_version or '2025'}"
            else:
                release_id = "rvu_gpci_2025"
        
        if not batch_id:
            batch_id = f"batch_{int(datetime.now().timestamp())}"
        
        logger.info("Loading to gpci table", release_id=release_id, batch_id=batch_id)
        
        # Extract year from data (use most common year or first record)
        years = [idx.effective_start.year if idx.effective_start else 2025 for idx in gpci_indices]
        year = max(set(years), key=years.count) if years else 2025
        
        # Clear existing data for this year
        deleted = db.query(GPCI).filter(GPCI.year == year).delete(synchronize_session=False)
        logger.info(f"Cleared {deleted} existing rows for year {year}")
        
        # Transform and load
        # Note: gpci_indices has (mac, locality_id, effective_start) as NK
        #       gpci simplified table has (year, locality_id, effective_from) as NK
        #       Multiple MACs can share same locality_id, so we deduplicate
        #       by picking one representative row per (locality_id, effective_from)
        
        seen_keys = set()  # Track (locality_id, effective_from) to avoid duplicates
        records_loaded = 0
        
        for idx in gpci_indices:
            try:
                # Extract year and effective date
                record_year = idx.effective_start.year if idx.effective_start else year
                effective_from = idx.effective_start
                
                # Create deduplication key (simplified table NK)
                dedupe_key = (str(idx.locality_id), effective_from)
                
                # Skip if we've already loaded this (locality_id, effective_from)
                if dedupe_key in seen_keys:
                    logger.debug("Skipping duplicate locality/date",
                               locality_id=idx.locality_id,
                               effective_from=effective_from,
                               mac=idx.mac)
                    continue
                
                # Validate required fields
                if not idx.work_gpci or not idx.pe_gpci or not idx.mp_gpci:
                    logger.warning("Skipping row with missing GPCI values",
                                 locality_id=idx.locality_id,
                                 mac=idx.mac)
                    continue
                
                # Map from gpci_indices to gpci schema
                gpci_record = GPCI(
                    year=record_year,
                    locality_id=str(idx.locality_id),
                    locality_name=idx.locality_name or "",
                    gpci_work=float(idx.work_gpci),
                    gpci_pe=float(idx.pe_gpci),
                    gpci_mp=float(idx.mp_gpci),
                    effective_from=effective_from,
                    effective_to=idx.effective_end,
                    release_id=release_id,
                    batch_id=batch_id
                )
                
                db.add(gpci_record)
                seen_keys.add(dedupe_key)
                records_loaded += 1
                
                # Commit in batches
                if records_loaded % 100 == 0:
                    db.commit()
                    logger.debug(f"Committed {records_loaded} records...")
                    
            except Exception as e:
                logger.warning("Failed to transform/load GPCI record",
                            error=str(e),
                            locality_id=idx.locality_id,
                            mac=idx.mac)
                continue
        
        # Final commit
        db.commit()
        
        logger.info(f"✅ Loaded {records_loaded} rows to gpci table",
                   release_id=release_id,
                   batch_id=batch_id,
                   year=year)
        
        return records_loaded
        
    except Exception as e:
        db.rollback()
        logger.error("Failed to load GPCI data", error=str(e))
        raise
        
    finally:
        db.close()


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Load GPCI data from gpci_indices to gpci table"
    )
    parser.add_argument(
        '--release-id',
        default=None,
        help='Release identifier (defaults to inferred from data)'
    )
    parser.add_argument(
        '--batch-id',
        default=None,
        help='Batch identifier (defaults to timestamp-based)'
    )
    
    args = parser.parse_args()
    
    try:
        count = load_gpci_from_indices(
            release_id=args.release_id,
            batch_id=args.batch_id
        )
        
        print(f"\n✅ Successfully loaded {count} rows to gpci table")
        print(f"   Release ID: {args.release_id or 'auto'}")
        print(f"   Batch ID: {args.batch_id or 'auto'}")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

