#!/usr/bin/env python3
"""
Load RVU data to Render production database.
Usage: 
    python scripts/load_rvu_to_production.py
    python scripts/load_rvu_to_production.py --release-id rvu_2025_prod --output-dir data/ingestion/production

This script runs the complete RVU ingestion pipeline and loads data to production.
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from sqlalchemy.orm import Session

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from cms_pricing.database import SessionLocal
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor
from cms_pricing.models.rvu import Release

logger = logging.getLogger(__name__)


def main(release_id: str = None, output_dir: str = None):
    """Run RVU ingestion on production database.
    
    Args:
        release_id: Release ID to ingest (default: "rvu_2025_prod")
        output_dir: Output directory for parquet files. If not provided,
            the script will use RVU_OUTPUT_DIR env var, fall back to
            "/var/data/ingestion/production" when available, otherwise
            "data/ingestion/production".
    """
    
    # Use defaults if not provided
    release_id = release_id or "rvu_2025_prod"

    default_dirs = [
        output_dir,
        os.getenv("RVU_OUTPUT_DIR"),
        "/var/data/ingestion/production",
        "data/ingestion/production",
    ]
    for candidate in default_dirs:
        if candidate:
            output_dir = candidate
            break
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Create database session
    db_session = SessionLocal()
    
    try:
        logger.info("Starting RVU ingestion on production database")
        logger.info(f"Release ID: {release_id}")
        logger.info(f"Output directory: {output_dir}")
        
        # Optimization 1: Check if release already exists (short-circuit)
        source_version = release_id[:10]  # Truncate to match DB constraint (VARCHAR(10))
        existing_release = db_session.query(Release).filter_by(
            type="RVU_FULL",
            source_version=source_version
        ).first()
        
        if existing_release:
            logger.info(f"✅ Release {release_id} (source_version={source_version}) already exists in database. Skipping ingestion.")
            return 0
        
        # Initialize ingestor with production database session
        ingestor = RVUIngestor(output_dir=output_dir, db_session=db_session)
        
        # Generate batch ID for this ingestion run
        batch_id = f"batch_prod_{int(time.time())}"
        
        # Run ingestion (will use sample_data/rvu25a/ by default for testing)
        # In production, this would use scraper discovery
        logger.info(f"Running ingestion pipeline... (release_id={release_id}, batch_id={batch_id})")
        import asyncio
        result = asyncio.run(ingestor.ingest(release_id=release_id, batch_id=batch_id))
        
        # Verify results (log summary instead of full dict)
        status = result.get("status", "unknown")
        total_records = result.get("total_records", 0)
        logger.info(f"Ingestion completed: status={status}, total_records={total_records}")
        
        # Optimization 2: O(1) verification query (indexed lookup instead of full table scan)
        release_exists = db_session.query(Release).filter_by(
            type="RVU_FULL",
            source_version=source_version
        ).first() is not None
        
        if release_exists:
            logger.info("✅ Data successfully loaded to production database!")
            return 0
        else:
            logger.warning("⚠️  Release not found in database after ingestion. Ingestion may have failed.")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {e}", exc_info=True)
        return 1
        
    finally:
        db_session.close()
        logger.info("Database session closed")


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Optimization 3: CLI flags for --release-id and --output-dir
    parser = argparse.ArgumentParser(
        description="Load RVU data to Render production database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use defaults
  python scripts/load_rvu_to_production.py
  
  # Specify release ID
  python scripts/load_rvu_to_production.py --release-id rvu_2025_prod
  
  # Specify both release ID and output directory
  python scripts/load_rvu_to_production.py --release-id rvu_2025_prod --output-dir data/ingestion/production
        """
    )
    parser.add_argument(
        "--release-id",
        type=str,
        help="Release ID to ingest (default: rvu_2025_prod)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for parquet files (default: data/ingestion/production)"
    )
    
    args = parser.parse_args()
    sys.exit(main(release_id=args.release_id, output_dir=args.output_dir))
