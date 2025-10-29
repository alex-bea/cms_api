#!/usr/bin/env python3
"""
Load RVU data to Render production database.
Usage: python scripts/load_rvu_to_production.py

This script runs the complete RVU ingestion pipeline and loads data to production.
"""

import sys
import logging
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


def main():
    """Run RVU ingestion on production database."""
    
    # Output directory for parquet files
    output_dir = "data/ingestion/production"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Create database session
    db_session = SessionLocal()
    
    try:
        logger.info("Starting RVU ingestion on production database")
        logger.info(f"Output directory: {output_dir}")
        
        # Initialize ingestor with production database session
        ingestor = RVUIngestor(output_dir=output_dir, db_session=db_session)
        
        # Generate IDs for this ingestion run
        release_id = "rvu_2025_prod"
        batch_id = f"batch_prod_{int(time.time())}"
        
        # Run ingestion (will use sample_data/rvu25a/ by default for testing)
        # In production, this would use scraper discovery
        logger.info(f"Running ingestion pipeline... (release_id={release_id}, batch_id={batch_id})")
        import asyncio
        result = asyncio.run(ingestor.ingest(release_id=release_id, batch_id=batch_id))
        
        # Verify results
        logger.info(f"Ingestion completed: {result}")
        
        # Check database
        release_count = db_session.query(Release).count()
        logger.info(f"Total releases in database: {release_count}")
        
        if release_count > 0:
            logger.info("✅ Data successfully loaded to production database!")
            return 0
        else:
            logger.warning("⚠️  No releases found in database. Ingestion may have failed.")
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
    
    sys.exit(main())

