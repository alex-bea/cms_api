#!/usr/bin/env python3
"""
Load GPCI data from sample_data to production database.

This script:
1. Parses GPCI2025.txt with the fixed v1.3 parser
2. Loads to gpci_indices table with correct column mapping
3. Then loads to simplified gpci table
"""

import sys
import uuid
from pathlib import Path
from datetime import datetime
import pandas as pd
import structlog

sys.path.insert(0, str(Path(__file__).parent.parent))

from cms_pricing.database import SessionLocal
from cms_pricing.ingestion.parsers.gpci_parser import parse_gpci
from cms_pricing.models.rvu import Release, GPCIIndex
from cms_pricing.models.fee_schedules import GPCI

logger = structlog.get_logger()


def load_gpci_from_sample():
    """Load GPCI from sample_data file to database."""
    
    # Parse GPCI file
    source_file = Path("sample_data/rvu25d_0/GPCI2025.txt")
    if not source_file.exists():
        logger.error("GPCI source file not found", path=str(source_file))
        return 1
    
    logger.info("Parsing GPCI file", file=str(source_file))
    
    metadata = {
        'release_id': 'rvu_2025_prod',
        'schema_id': 'cms_gpci_v1.3',
        'product_year': '2025',
        'quarter_vintage': 'D',
        'vintage_date': datetime(2025, 10, 1),
        'file_sha256': 'sample_data_sha256',
        'source_uri': str(source_file),
        'source_release': 'RVU25D',
    }
    
    with open(source_file, 'rb') as f:
        result = parse_gpci(f, source_file.name, metadata)
    
    logger.info("Parsed GPCI data", 
               valid_rows=len(result.data),
               rejects=len(result.rejects))
    
    if len(result.data) == 0:
        logger.error("No valid GPCI data parsed")
        return 1
    
    # Load to database
    db = SessionLocal()
    
    try:
        # Create or get release
        release_record = db.query(Release).filter(
            Release.source_version == 'rvu_2025_prod'
        ).first()
        
        if not release_record:
            release_uuid = uuid.uuid4()
            release_record = Release(
                id=release_uuid,
                type="RVU_FULL",
                source_version='rvu_2025_prod',
                imported_at=datetime.utcnow().date(),
                notes='manual_gpci_load'
            )
            db.add(release_record)
            db.flush()
        else:
            release_uuid = release_record.id
        
        batch_id = f"batch_gpci_{int(datetime.now().timestamp())}"
        
        # Load to gpci_indices
        logger.info("Loading to gpci_indices table", release_uuid=release_uuid)
        
        records_loaded = 0
        for idx, row in result.data.iterrows():
            try:
                gpci_record = GPCIIndex(
                    id=uuid.uuid4(),
                    release_id=release_uuid,
                    mac=str(row.get('mac', '')),
                    state=str(row.get('state', '')),
                    locality_id=str(row.get('locality_code', '')),
                    locality_name=str(row.get('locality_name', '')),
                    work_gpci=float(row['gpci_work']) if row['gpci_work'] else None,
                    pe_gpci=float(row['gpci_pe']) if row['gpci_pe'] else None,
                    mp_gpci=float(row['gpci_mp']) if row['gpci_mp'] else None,
                    effective_start=pd.to_datetime(row['effective_from']).date() if row.get('effective_from') else None,
                    effective_end=pd.to_datetime(row['effective_to']).date() if row.get('effective_to') else None,
                    source_file=source_file.name,
                    row_num=int(idx) if isinstance(idx, (int, float)) else None
                )
                
                db.add(gpci_record)
                records_loaded += 1
                
                if records_loaded % 50 == 0:
                    db.commit()
                    logger.debug(f"Committed {records_loaded} records...")
                    
            except Exception as e:
                logger.warning("Failed to load GPCI record", error=str(e), row_idx=idx)
                continue
        
        db.commit()
        logger.info(f"✅ Loaded {records_loaded} rows to gpci_indices")
        
        # Now load to simplified gpci table
        logger.info("Loading to simplified gpci table")
        
        # Get all gpci_indices
        gpci_indices = db.query(GPCIIndex).all()
        
        if not gpci_indices:
            logger.error("No data in gpci_indices to load")
            return 1
        
        # Get year from data
        years = [idx.effective_start.year if idx.effective_start else 2025 for idx in gpci_indices]
        year = max(set(years), key=years.count) if years else 2025
        
        # Clear existing data for this year
        deleted = db.query(GPCI).filter(GPCI.year == year).delete(synchronize_session=False)
        logger.info(f"Cleared {deleted} existing rows")
        
        # Deduplicate and load
        seen_keys = set()
        records_loaded_simplified = 0
        release_id_str = "rvu_gpci_2025"
        batch_id_simplified = f"batch_{int(datetime.now().timestamp())}"
        
        for idx in gpci_indices:
            try:
                record_year = idx.effective_start.year if idx.effective_start else year
                effective_from = idx.effective_start
                
                if not effective_from:
                    continue
                
                dedupe_key = (str(idx.locality_id), effective_from)
                
                if dedupe_key in seen_keys:
                    continue
                
                if not idx.work_gpci or not idx.pe_gpci or not idx.mp_gpci:
                    continue
                
                gpci_record = GPCI(
                    year=record_year,
                    locality_id=str(idx.locality_id),
                    locality_name=idx.locality_name or "",
                    gpci_work=float(idx.work_gpci),
                    gpci_pe=float(idx.pe_gpci),
                    gpci_mp=float(idx.mp_gpci),
                    effective_from=effective_from,
                    effective_to=idx.effective_end,
                    release_id=release_id_str,
                    batch_id=batch_id_simplified
                )
                
                db.add(gpci_record)
                seen_keys.add(dedupe_key)
                records_loaded_simplified += 1
                
                if records_loaded_simplified % 50 == 0:
                    db.commit()
                    logger.debug(f"Committed {records_loaded_simplified} simplified records...")
                    
            except Exception as e:
                logger.warning("Failed to load simplified GPCI record", error=str(e))
                continue
        
        db.commit()
        logger.info(f"✅ Loaded {records_loaded_simplified} rows to gpci table")
        
        return 0
        
    except Exception as e:
        db.rollback()
        logger.error("Failed to load GPCI", error=str(e))
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(load_gpci_from_sample())

