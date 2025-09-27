#!/usr/bin/env python3
"""Load ingested data into the database"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
from typing import List

# Add the project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

from cms_pricing.database import SessionLocal, engine
from cms_pricing.models.geography import Geography
from cms_pricing.models.fee_schedules import FeeMPFS, GPCI, ConversionFactor
import structlog

logger = structlog.get_logger()


class DatabaseLoader:
    """Loads ingested data files into the database"""
    
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = Path(data_dir)
    
    def load_geography_data(self, build_id: str = None) -> int:
        """Load geography data from parquet files"""
        
        # Find the most recent geography build if not specified
        if build_id is None:
            geo_dir = self.data_dir / "GEOGRAPHY"
            if not geo_dir.exists():
                logger.error("No GEOGRAPHY data directory found")
                return 0
            
            builds = [d for d in geo_dir.iterdir() if d.is_dir()]
            if not builds:
                logger.error("No GEOGRAPHY builds found")
                return 0
            
            # Sort by creation time and get the most recent
            build_id = max(builds, key=lambda x: x.stat().st_mtime).name
            logger.info("Using most recent geography build", build_id=build_id)
        
        # Load the parquet file
        parquet_file = self.data_dir / "GEOGRAPHY" / build_id / "normalized" / "geography.parquet"
        if not parquet_file.exists():
            logger.error("Geography parquet file not found", file=str(parquet_file))
            return 0
        
        logger.info("Loading geography data", file=str(parquet_file))
        df = pd.read_parquet(parquet_file)
        
        # Clear existing geography data
        db = SessionLocal()
        try:
            db.query(Geography).delete()
            logger.info("Cleared existing geography data")
            
            # Insert new data
            records_created = 0
            for _, row in df.iterrows():
                geo_record = Geography(
                    zip5=row['zip5'],
                    plus4=row.get('plus4'),  # ZIP+4 add-on
                    has_plus4=row.get('has_plus4', 0),  # 1 if plus4 present, 0 if ZIP5-only
                    state=row.get('state'),  # State code
                    locality_id=row['locality_id'],
                    locality_name=row['locality_name'],
                    carrier=row.get('carrier'),  # MAC/Carrier jurisdiction code
                    rural_flag=row.get('rural_flag'),  # R, B, or blank for DMEPOS/rural logic
                    effective_from=datetime.strptime(row['effective_from'], '%Y-%m-%d').date(),
                    effective_to=None,
                    dataset_id=row.get('dataset_id', 'ZIP_LOCALITY'),
                    dataset_digest=row.get('dataset_digest') or 'unknown',
                    created_at=row['created_at']
                )
                db.add(geo_record)
                records_created += 1
            
            db.commit()
            logger.info("Geography data loaded successfully", records=records_created)
            return records_created
            
        except Exception as e:
            db.rollback()
            logger.error("Error loading geography data", error=str(e))
            raise
        finally:
            db.close()
    
    def load_mpfs_data(self, build_id: str = None) -> int:
        """Load MPFS data from parquet files"""
        
        # Find the most recent MPFS build if not specified
        if build_id is None:
            mpfs_dir = self.data_dir / "MPFS"
            if not mpfs_dir.exists():
                logger.error("No MPFS data directory found")
                return 0
            
            builds = [d for d in mpfs_dir.iterdir() if d.is_dir()]
            if not builds:
                logger.error("No MPFS builds found")
                return 0
            
            build_id = max(builds, key=lambda x: x.stat().st_mtime).name
            logger.info("Using most recent MPFS build", build_id=build_id)
        
        db = SessionLocal()
        try:
            records_created = 0
            
            # Load fee_mpfs data
            fee_file = self.data_dir / "MPFS" / build_id / "normalized" / "fee_mpfs.parquet"
            if fee_file.exists():
                logger.info("Loading MPFS fee data", file=str(fee_file))
                df = pd.read_parquet(fee_file)
                
                # Clear existing data for this year
                year = int(df['year'].iloc[0]) if 'year' in df.columns else None
                if year:
                    db.query(FeeMPFS).filter(FeeMPFS.year == year).delete()
                
                for _, row in df.iterrows():
                    fee_record = FeeMPFS(
                        hcpcs=row['hcpcs'],
                        work_rvu=row['work_rvu'],
                        pe_nf_rvu=row['pe_nf_rvu'],
                        pe_fac_rvu=row['pe_fac_rvu'],
                        mp_rvu=row['mp_rvu'],
                        global_days=row.get('global_days', 0),
                        status_indicator=row.get('status_indicator', ''),
                        year=row['year'],
                        revision=row.get('revision', 'A'),
                        effective_from=datetime.strptime(row['effective_from'], '%Y-%m-%d').date(),
                        effective_to=None
                    )
                    db.add(fee_record)
                    records_created += 1
            
            # Load GPCI data
            gpci_file = self.data_dir / "MPFS" / build_id / "normalized" / "gpci.parquet"
            if gpci_file.exists():
                logger.info("Loading GPCI data", file=str(gpci_file))
                df = pd.read_parquet(gpci_file)
                
                year = int(df['year'].iloc[0]) if 'year' in df.columns else None
                if year:
                    db.query(GPCI).filter(GPCI.year == year).delete()
                
                for _, row in df.iterrows():
                    gpci_record = GPCI(
                        locality_id=row['locality_id'],
                        locality_name=row['locality_name'],
                        gpci_work=row['gpci_work'],
                        gpci_pe=row['gpci_pe'],
                        gpci_mp=row['gpci_mp'],
                        year=row['year'],
                        effective_from=datetime.strptime(row['effective_from'], '%Y-%m-%d').date(),
                        effective_to=None
                    )
                    db.add(gpci_record)
                    records_created += 1
            
            # Load conversion factor data
            cf_file = self.data_dir / "MPFS" / build_id / "normalized" / "conversion_factor.parquet"
            if cf_file.exists():
                logger.info("Loading conversion factor data", file=str(cf_file))
                df = pd.read_parquet(cf_file)
                
                year = int(df['year'].iloc[0]) if 'year' in df.columns else None
                if year:
                    db.query(ConversionFactor).filter(ConversionFactor.year == year).delete()
                
                for _, row in df.iterrows():
                    cf_record = ConversionFactor(
                        cf=row['cf'],
                        source=row.get('source', 'MPFS'),
                        year=row['year'],
                        effective_from=datetime.strptime(row['effective_from'], '%Y-%m-%d').date(),
                        effective_to=None
                    )
                    db.add(cf_record)
                    records_created += 1
            
            db.commit()
            logger.info("MPFS data loaded successfully", records=records_created)
            return records_created
            
        except Exception as e:
            db.rollback()
            logger.error("Error loading MPFS data", error=str(e))
            raise
        finally:
            db.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Load ingested data into database")
    parser.add_argument("--dataset", choices=["GEOGRAPHY", "MPFS", "ALL"], default="ALL", 
                       help="Dataset to load")
    parser.add_argument("--build-id", help="Specific build ID to load")
    parser.add_argument("--data-dir", default="./data", help="Data directory path")
    
    args = parser.parse_args()
    
    loader = DatabaseLoader(args.data_dir)
    
    if args.dataset in ["GEOGRAPHY", "ALL"]:
        print("🔄 Loading geography data...")
        geo_count = loader.load_geography_data(args.build_id)
        print(f"✅ Loaded {geo_count} geography records")
    
    if args.dataset in ["MPFS", "ALL"]:
        print("🔄 Loading MPFS data...")
        mpfs_count = loader.load_mpfs_data(args.build_id)
        print(f"✅ Loaded {mpfs_count} MPFS records")
    
    print("🎉 Data loading completed!")


if __name__ == "__main__":
    main()
