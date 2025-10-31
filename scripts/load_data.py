#!/usr/bin/env python3
"""Load ingested data into the database"""

import sys
import os
import re
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
    
    def _extract_provenance(self, df: pd.DataFrame, build_id: str = None, 
                           dataset_prefix: str = None) -> tuple[str | None, str | None]:
        """
        Extract release_id and batch_id from DataFrame or infer from build_id.
        
        Args:
            df: DataFrame that may contain release_id/batch_id columns
            build_id: Build identifier to use as fallback
            dataset_prefix: Dataset prefix for release_id fallback (e.g., 'mpfs', 'opps', 'asc').
                          If None, inferred from build_id path.
            
        Returns:
            Tuple of (release_id, batch_id). Both may be None if unavailable.
        """
        # Try to extract from DataFrame columns (if preserved from ingestion)
        release_id = None
        batch_id = None
        
        # Check if columns exist and have non-null values
        if 'release_id' in df.columns:
            # Get first non-null value (should be same for all rows in a batch)
            release_values = df['release_id'].dropna().unique()
            if len(release_values) > 0:
                release_id = str(release_values[0])
                # Warn if multiple release_ids found (indicates mixed vintages - likely a bug)
                if len(release_values) > 1:
                    logger.warning(
                        "Multiple release_id values found in DataFrame - this may indicate mixed vintages",
                        release_ids=list(release_values),
                        count=len(release_values),
                        using_first=release_id
                    )
        
        if 'batch_id' in df.columns:
            batch_values = df['batch_id'].dropna().unique()
            if len(batch_values) > 0:
                batch_id = str(batch_values[0])
                # Warn if multiple batch_ids found (indicates mixed batches)
                if len(batch_values) > 1:
                    logger.warning(
                        "Multiple batch_id values found in DataFrame",
                        batch_ids=list(batch_values),
                        count=len(batch_values),
                        using_first=batch_id
                    )
        
        # Fallback: infer from build_id if metadata missing
        if not release_id and build_id:
            # Infer dataset prefix from build_id if not provided
            if not dataset_prefix:
                # Extract from path like "MPFS/2025-01-15" -> "mpfs"
                # or "OPPS/2025Q1" -> "opps"
                path_parts = build_id.split('/')
                if len(path_parts) > 0:
                    dataset_prefix = path_parts[0].lower()
                else:
                    # Fallback: try to infer from build_id string
                    # Pre-populated map to reduce warnings for common datasets
                    build_lower = build_id.lower()
                    if 'mpfs' in build_lower:
                        dataset_prefix = 'mpfs'
                    elif 'opps' in build_lower:
                        dataset_prefix = 'opps'
                    elif 'asc' in build_lower:
                        dataset_prefix = 'asc'
                    elif 'clfs' in build_lower:
                        dataset_prefix = 'clfs'
                    elif 'dmepos' in build_lower or 'dme' in build_lower:
                        dataset_prefix = 'dmepos'
                    elif 'ipps' in build_lower:
                        dataset_prefix = 'ipps'
                    else:
                        dataset_prefix = 'unknown'
                        logger.warning(
                            "Could not infer dataset prefix from build_id - using 'unknown'",
                            build_id=build_id,
                            suggestion="Pass dataset_prefix explicitly (e.g., 'mpfs', 'opps', 'asc', 'clfs', 'dmepos', 'ipps')"
                        )
            
            # Try to extract year from DataFrame, build_id path, or date pattern
            year = None
            if 'year' in df.columns:
                year_values = df['year'].dropna().unique()
                if len(year_values) > 0:
                    year = str(int(year_values[0]))
            
            # Fallback: extract year from build_id directory name (e.g., "2025-01-15" -> "2025")
            if not year and build_id:
                # Try to match YYYY-MM-DD or YYYY-MM pattern
                year_match = re.search(r'(\d{4})-\d{2}', build_id)
                if year_match:
                    year = year_match.group(1)
                    logger.debug("Extracted year from build_id path", year=year, build_id=build_id)
            
            if year:
                # Infer release_id from build_id pattern
                # e.g., "MPFS/2025-01-15_123456" -> "mpfs_2025_legacy_2025-01-15_123456"
                # e.g., "OPPS/2025Q1" -> "opps_2025_legacy_2025Q1"
                sanitized_build_id = build_id.replace('/', '_').replace('\\', '_')
                release_id = f"{dataset_prefix}_{year}_legacy_{sanitized_build_id}"
            
            # Use build_id as batch_id fallback
            if not batch_id:
                batch_id = build_id
        
        return release_id, batch_id
    
    def _prepare_bulk_records(self, df: pd.DataFrame,
                              release_id: str | None, batch_id: str | None,
                              field_mappings: dict) -> list[dict]:
        """
        Convert DataFrame to list of dicts ready for bulk_insert_mappings.
        
        Uses vectorized operations and to_dict('records') for maximum performance.
        
        Args:
            df: Source DataFrame (will be copied to avoid modifying original)
            release_id: Batch-level release_id (used as fallback)
            batch_id: Batch-level batch_id (used as fallback)
            field_mappings: Dict mapping DataFrame columns to model fields
            
        Returns:
            List of dicts ready for bulk_insert_mappings
        """
        # Work on a copy to avoid modifying original DataFrame
        df_work = df.copy()
        
        # Pre-process dates once (vectorized - much faster than per-row)
        if 'effective_from' in df_work.columns:
            if df_work['effective_from'].dtype == 'object':
                df_work['effective_from'] = pd.to_datetime(df_work['effective_from']).dt.date
        
        # Add provenance columns if not present (vectorized)
        if release_id and 'release_id' not in df_work.columns:
            df_work['release_id'] = release_id
        if batch_id and 'batch_id' not in df_work.columns:
            df_work['batch_id'] = batch_id
        
        # Select only columns we need (reduces memory and processing)
        needed_cols = list(field_mappings.keys()) + ['release_id', 'batch_id']
        needed_cols = [col for col in needed_cols if col in df_work.columns]
        df_subset = df_work[needed_cols]
        
        # Convert to list of dicts (much faster than iterrows)
        records_dicts = df_subset.to_dict('records')
        
        # Map DataFrame columns to model fields
        records = []
        for record_dict in records_dicts:
            mapped_record = {}
            
            # Map fields according to mapping
            for df_col, model_field in field_mappings.items():
                if df_col in record_dict:
                    value = record_dict[df_col]
                    # Handle NaN values (convert to None for SQLAlchemy)
                    if pd.isna(value):
                        mapped_record[model_field] = None
                    else:
                        mapped_record[model_field] = value
            
            # Handle provenance fields with fallback
            if 'release_id' in record_dict and record_dict['release_id'] and not pd.isna(record_dict['release_id']):
                mapped_record['release_id'] = str(record_dict['release_id'])
            elif release_id:
                mapped_record['release_id'] = release_id
            
            if 'batch_id' in record_dict and record_dict['batch_id'] and not pd.isna(record_dict['batch_id']):
                mapped_record['batch_id'] = str(record_dict['batch_id'])
            elif batch_id:
                mapped_record['batch_id'] = batch_id
            
            records.append(mapped_record)
        
        return records
    
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
            db.query(Geography).delete(synchronize_session=False)
            logger.info("Cleared existing geography data")
            
            # Prepare bulk insert records
            field_mappings = {
                'zip5': 'zip5',
                'plus4': 'plus4',
                'has_plus4': 'has_plus4',
                'state': 'state',
                'locality_id': 'locality_id',
                'locality_name': 'locality_name',
                'carrier': 'carrier',
                'rural_flag': 'rural_flag',
                'effective_from': 'effective_from',
                'dataset_id': 'dataset_id',
                'dataset_digest': 'dataset_digest',
                'created_at': 'created_at'
            }
            
            # Pre-process dates
            if 'effective_from' in df.columns and df['effective_from'].dtype == 'object':
                df['effective_from'] = pd.to_datetime(df['effective_from']).dt.date
            
            records = self._prepare_bulk_records(df, None, None, field_mappings)
            
            # Bulk insert (much faster than individual adds)
            if records:
                db.bulk_insert_mappings(Geography, records)
                records_created = len(records)
            
            db.commit()
            logger.info("Geography data loaded successfully", records=records_created)
            return records_created
            
        except Exception as e:
            db.rollback()
            logger.error("Error loading geography data", error=str(e))
            raise
        finally:
            db.close()
    
    def load_mpfs_data(self, build_id: str = None, dataset_prefix: str = None) -> int:
        """Load MPFS data from parquet files
        
        Args:
            build_id: Specific build ID to load (defaults to most recent)
            dataset_prefix: Dataset prefix for provenance (defaults to 'mpfs' if not provided)
        """
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
        
        # Default to 'mpfs' prefix if not provided
        if dataset_prefix is None:
            dataset_prefix = 'mpfs'
        
        # Use try/except/finally to ensure session cleanup
        db = SessionLocal()
        try:
            records_created = 0
            
            # Load fee_mpfs data
            fee_file = self.data_dir / "MPFS" / build_id / "normalized" / "fee_mpfs.parquet"
            if fee_file.exists():
                logger.info("Loading MPFS fee data", file=str(fee_file))
                df = pd.read_parquet(fee_file)
                
                # Extract provenance metadata (with dataset prefix)
                release_id, batch_id = self._extract_provenance(df, build_id, dataset_prefix=dataset_prefix)
                if release_id or batch_id:
                    logger.info("Extracted provenance metadata", 
                              release_id=release_id, batch_id=batch_id)
                else:
                    logger.warning("No provenance metadata found in MPFS data", 
                                 build_id=build_id)
                
                # Clear existing data for this year
                year = int(df['year'].iloc[0]) if 'year' in df.columns else None
                if year:
                    db.query(FeeMPFS).filter(FeeMPFS.year == year).delete(synchronize_session=False)
                
                # Prepare bulk insert records
                field_mappings = {
                    'hcpcs': 'hcpcs',
                    'work_rvu': 'work_rvu',
                    'pe_nf_rvu': 'pe_nf_rvu',
                    'pe_fac_rvu': 'pe_fac_rvu',
                    'mp_rvu': 'mp_rvu',
                    'global_days': 'global_days',
                    'status_indicator': 'status_indicator',
                    'year': 'year',
                    'revision': 'revision',
                    'effective_from': 'effective_from'
                }
                
                records = self._prepare_bulk_records(df, release_id, batch_id, field_mappings)
                
                # Set defaults for fields not in DataFrame
                for record in records:
                    if 'global_days' not in record:
                        record['global_days'] = 0
                    if 'status_indicator' not in record:
                        record['status_indicator'] = ''
                    if 'revision' not in record:
                        record['revision'] = 'A'
                
                # Bulk insert (chunked for very large datasets)
                if records:
                    chunk_size = 10000  # Insert in chunks to manage memory and transaction size
                    total_records = len(records)
                    
                    for i in range(0, total_records, chunk_size):
                        chunk = records[i:i + chunk_size]
                        db.bulk_insert_mappings(FeeMPFS, chunk)
                        logger.debug("Inserted chunk", chunk_size=len(chunk), 
                                    total_progress=f"{min(i+chunk_size, total_records)}/{total_records}")
                    
                    records_created += total_records
            
            # Load GPCI data
            gpci_file = self.data_dir / "MPFS" / build_id / "normalized" / "gpci.parquet"
            if gpci_file.exists():
                logger.info("Loading GPCI data", file=str(gpci_file))
                df = pd.read_parquet(gpci_file)
                
                # Extract provenance metadata (with dataset prefix)
                release_id, batch_id = self._extract_provenance(df, build_id, dataset_prefix=dataset_prefix)
                
                year = int(df['year'].iloc[0]) if 'year' in df.columns else None
                if year:
                    db.query(GPCI).filter(GPCI.year == year).delete(synchronize_session=False)
                
                # Prepare bulk insert records
                field_mappings = {
                    'locality_id': 'locality_id',
                    'locality_name': 'locality_name',
                    'gpci_work': 'gpci_work',
                    'gpci_pe': 'gpci_pe',
                    'gpci_mp': 'gpci_mp',
                    'year': 'year',
                    'effective_from': 'effective_from'
                }
                
                records = self._prepare_bulk_records(df, release_id, batch_id, field_mappings)
                
                # Bulk insert (chunked for very large datasets)
                if records:
                    chunk_size = 10000
                    total_records = len(records)
                    
                    for i in range(0, total_records, chunk_size):
                        chunk = records[i:i + chunk_size]
                        db.bulk_insert_mappings(GPCI, chunk)
                    
                    records_created += total_records
            
            # Load conversion factor data
            cf_file = self.data_dir / "MPFS" / build_id / "normalized" / "conversion_factor.parquet"
            if cf_file.exists():
                logger.info("Loading conversion factor data", file=str(cf_file))
                df = pd.read_parquet(cf_file)
                
                # Extract provenance metadata (with dataset prefix)
                release_id, batch_id = self._extract_provenance(df, build_id, dataset_prefix=dataset_prefix)
                
                year = int(df['year'].iloc[0]) if 'year' in df.columns else None
                if year:
                    db.query(ConversionFactor).filter(ConversionFactor.year == year).delete(synchronize_session=False)
                
                # Prepare bulk insert records
                field_mappings = {
                    'cf': 'cf',
                    'source': 'source',
                    'year': 'year',
                    'effective_from': 'effective_from'
                }
                
                records = self._prepare_bulk_records(df, release_id, batch_id, field_mappings)
                
                # Set defaults for fields not in DataFrame
                for record in records:
                    if 'source' not in record:
                        record['source'] = 'MPFS'
                
                # Bulk insert (chunked for very large datasets)
                if records:
                    chunk_size = 10000
                    total_records = len(records)
                    
                    for i in range(0, total_records, chunk_size):
                        chunk = records[i:i + chunk_size]
                        db.bulk_insert_mappings(ConversionFactor, chunk)
                    
                    records_created += total_records
            
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
    parser.add_argument("--dataset-prefix", 
                       choices=["mpfs", "opps", "asc", "clfs", "dmepos", "ipps"],
                       help="Dataset prefix for provenance (e.g., 'mpfs', 'opps'). "
                            "If not provided, inferred from build path.")
    
    args = parser.parse_args()
    
    loader = DatabaseLoader(args.data_dir)
    
    if args.dataset in ["GEOGRAPHY", "ALL"]:
        print("🔄 Loading geography data...")
        geo_count = loader.load_geography_data(args.build_id)
        print(f"✅ Loaded {geo_count} geography records")
    
    if args.dataset in ["MPFS", "ALL"]:
        print("🔄 Loading MPFS data...")
        mpfs_count = loader.load_mpfs_data(args.build_id, dataset_prefix=args.dataset_prefix)
        print(f"✅ Loaded {mpfs_count} MPFS records")
    
    print("🎉 Data loading completed!")


if __name__ == "__main__":
    main()
