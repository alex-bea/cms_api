"""ETL pipeline for nearest ZIP resolver data sources per PRD v1.0"""

import asyncio
import hashlib
import logging
import re
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlparse
import uuid

import httpx
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.database import SessionLocal
from cms_pricing.models.nearest_zip import (
    ZCTACoords, ZipToZCTA, CMSZipLocality, ZIP9Overrides,
    ZCTADistances, ZipMetadata, IngestRun
)

logger = logging.getLogger(__name__)


class NearestZipETL:
    """ETL pipeline for nearest ZIP resolver data sources"""
    
    def __init__(self, db_session: Optional[Session] = None):
        self.db = db_session or SessionLocal()
        self.current_run_id = str(uuid.uuid4())
        self.tool_version = "1.0.0"
    
    async def run_full_etl(self) -> Dict[str, Any]:
        """Run complete ETL pipeline for all data sources"""
        results = {}
        
        try:
            # 1. Gazetteer (ZCTA centroids)
            logger.info("Starting Gazetteer ETL")
            results['gazetteer'] = await self.etl_gazetteer()
            
            # 2. UDS ZIP↔ZCTA crosswalk
            logger.info("Starting UDS ZIP↔ZCTA ETL")
            results['uds_crosswalk'] = await self.etl_uds_crosswalk()
            
            # 3. CMS ZIP5 locality mapping
            logger.info("Starting CMS ZIP5 ETL")
            results['cms_zip5'] = await self.etl_cms_zip5()
            
            # 4. CMS ZIP9 overrides
            logger.info("Starting CMS ZIP9 ETL")
            results['cms_zip9'] = await self.etl_cms_zip9()
            
            # 5. SimpleMaps ZIP metadata
            logger.info("Starting SimpleMaps ETL")
            results['simplemaps'] = await self.etl_simplemaps()
            
            # 6. NBER distances (optional)
            logger.info("Starting NBER distances ETL")
            results['nber_distances'] = await self.etl_nber_distances()
            
            logger.info("ETL pipeline completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
        finally:
            if not self.db:
                self.db.close()
    
    async def etl_gazetteer(self) -> Dict[str, Any]:
        """ETL for Census Gazetteer ZCTA centroids"""
        url = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2025_Gazetteer/2025_Gaz_zcta_national.zip"
        filename = "2025_Gaz_zcta_national.zip"
        
        # Download and extract
        data = await self._download_file(url)
        extracted_data = await self._extract_zip(data)
        
        # Find the TXT file
        txt_file = None
        for file_path in extracted_data:
            if file_path.endswith('.txt'):
                txt_file = file_path
                break
        
        if not txt_file:
            raise ValueError("No TXT file found in Gazetteer ZIP")
        
        # Parse pipe-delimited TXT
        df = pd.read_csv(txt_file, sep='|', dtype=str)
        
        # Map columns to our schema
        records = []
        for _, row in df.iterrows():
            record = {
                'zcta5': row['GEOID'][:5],  # First 5 chars of GEOID
                'lat': float(row['INTPTLAT']),
                'lon': float(row['INTPTLONG']),
                'vintage': '2025',
                'source_filename': filename,
                'ingest_run_id': self.current_run_id
            }
            records.append(record)
        
        # Clear existing data for this vintage
        self.db.query(ZCTACoords).filter(ZCTACoords.vintage == '2025').delete()
        
        # Insert new data
        for record in records:
            zcta_coord = ZCTACoords(**record)
            self.db.add(zcta_coord)
        
        self.db.commit()
        
        # Log ingest run
        await self._log_ingest_run(
            source_url=url,
            filename=filename,
            row_count=len(records),
            status='success'
        )
        
        return {
            'source': 'gazetteer',
            'records_processed': len(records),
            'vintage': '2025'
        }
    
    async def etl_uds_crosswalk(self) -> Dict[str, Any]:
        """ETL for UDS/GeoCare ZIP↔ZCTA crosswalk"""
        url = "https://data.hrsa.gov/DataDownload/GeoCareNavigator/ZIP%20Code%20to%20ZCTA%20Crosswalk.xlsx"
        filename = "ZIP Code to ZCTA Crosswalk.xlsx"
        
        # Download file
        data = await self._download_file(url)
        
        # Parse Excel file
        df = pd.read_excel(data, dtype=str)
        
        records = []
        for _, row in df.iterrows():
            record = {
                'zip5': str(row['zip5']).zfill(5),
                'zcta5': str(row['zcta5']).zfill(5),
                'relationship': row.get('relationship'),
                'weight': float(row['weight']) if pd.notna(row.get('weight')) else None,
                'city': row.get('city'),
                'state': row.get('state'),
                'vintage': '2023',
                'source_filename': filename,
                'ingest_run_id': self.current_run_id
            }
            records.append(record)
        
        # Clear existing data for this vintage
        self.db.query(ZipToZCTA).filter(ZipToZCTA.vintage == '2023').delete()
        
        # Insert new data
        for record in records:
            zip_to_zcta = ZipToZCTA(**record)
            self.db.add(zip_to_zcta)
        
        self.db.commit()
        
        # Log ingest run
        await self._log_ingest_run(
            source_url=url,
            filename=filename,
            row_count=len(records),
            status='success'
        )
        
        return {
            'source': 'uds_crosswalk',
            'records_processed': len(records),
            'vintage': '2023'
        }
    
    async def etl_cms_zip5(self) -> Dict[str, Any]:
        """ETL for CMS ZIP5 locality mapping"""
        # This would need to be updated with actual CMS URLs
        # For now, using placeholder
        url = "https://www.cms.gov/medicare/payment/fee-schedules"
        filename = "Zip Code to Carrier Locality File – Revised 2025-08-14.zip"
        
        # In a real implementation, we'd:
        # 1. Scrape the CMS page to find the download link
        # 2. Download the ZIP file
        # 3. Extract and parse the CSV/TXT files
        # 4. Map to our schema
        
        # Placeholder implementation
        logger.warning("CMS ZIP5 ETL not yet implemented - requires CMS page scraping")
        
        return {
            'source': 'cms_zip5',
            'records_processed': 0,
            'vintage': '2025-08-14',
            'status': 'not_implemented'
        }
    
    async def etl_cms_zip9(self) -> Dict[str, Any]:
        """ETL for CMS ZIP9 overrides"""
        # Similar to ZIP5, requires CMS page scraping
        logger.warning("CMS ZIP9 ETL not yet implemented - requires CMS page scraping")
        
        return {
            'source': 'cms_zip9',
            'records_processed': 0,
            'vintage': '2025-08-14',
            'status': 'not_implemented'
        }
    
    async def etl_simplemaps(self) -> Dict[str, Any]:
        """ETL for SimpleMaps ZIP metadata"""
        # SimpleMaps requires form submission, so this would need to be done manually
        # or with a more sophisticated approach
        logger.warning("SimpleMaps ETL not yet implemented - requires manual download")
        
        return {
            'source': 'simplemaps',
            'records_processed': 0,
            'vintage': '2025-06-07',
            'status': 'not_implemented'
        }
    
    async def etl_nber_distances(self) -> Dict[str, Any]:
        """ETL for NBER ZCTA distances"""
        url = "https://data.nber.org/distance/zip/2024/100miles/gaz2024zcta5distance100miles.csv"
        filename = "gaz2024zcta5distance100miles.csv"
        
        # Download CSV
        data = await self._download_file(url)
        
        # Parse CSV
        df = pd.read_csv(data, dtype=str)
        
        records = []
        for _, row in df.iterrows():
            record = {
                'zcta5_a': str(row['zip1']).zfill(5),
                'zcta5_b': str(row['zip2']).zfill(5),
                'miles': float(row['mi_to_zcta5']),
                'vintage': '2024',
                'source_filename': filename,
                'ingest_run_id': self.current_run_id
            }
            records.append(record)
        
        # Clear existing data for this vintage
        self.db.query(ZCTADistances).filter(ZCTADistances.vintage == '2024').delete()
        
        # Insert new data
        for record in records:
            zcta_distance = ZCTADistances(**record)
            self.db.add(zcta_distance)
        
        self.db.commit()
        
        # Log ingest run
        await self._log_ingest_run(
            source_url=url,
            filename=filename,
            row_count=len(records),
            status='success'
        )
        
        return {
            'source': 'nber_distances',
            'records_processed': len(records),
            'vintage': '2024'
        }
    
    async def _download_file(self, url: str) -> bytes:
        """Download file from URL with retry logic"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    return response.content
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Download attempt {attempt + 1} failed: {e}, retrying in {retry_delay}s")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
    
    async def _extract_zip(self, zip_data: bytes) -> List[str]:
        """Extract ZIP file and return list of extracted file paths"""
        import zipfile
        import tempfile
        
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = Path(temp_dir) / "data.zip"
            zip_path.write_bytes(zip_data)
            
            extracted_files = []
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
                for file_path in zip_path.parent.rglob('*'):
                    if file_path.is_file():
                        extracted_files.append(str(file_path))
            
            return extracted_files
    
    async def _log_ingest_run(
        self,
        source_url: str,
        filename: str,
        row_count: int,
        status: str,
        notes: Optional[str] = None
    ) -> None:
        """Log an ingest run for provenance tracking"""
        ingest_run = IngestRun(
            run_id=self.current_run_id,
            source_url=source_url,
            filename=filename,
            sha256=None,  # Would calculate from downloaded data
            bytes=None,   # Would track from download
            started_at=datetime.now(),
            finished_at=datetime.now(),
            row_count=row_count,
            tool_version=self.tool_version,
            status=status,
            notes=notes
        )
        
        self.db.add(ingest_run)
        self.db.commit()


# CLI interface for running ETL
async def main():
    """CLI entry point for running ETL pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run nearest ZIP resolver ETL pipeline")
    parser.add_argument("--source", choices=[
        "gazetteer", "uds_crosswalk", "cms_zip5", "cms_zip9", 
        "simplemaps", "nber_distances", "all"
    ], default="all", help="Data source to process")
    
    args = parser.parse_args()
    
    etl = NearestZipETL()
    
    if args.source == "all":
        results = await etl.run_full_etl()
    else:
        # Run specific source
        method_name = f"etl_{args.source}"
        method = getattr(etl, method_name)
        results = await method()
    
    print(f"ETL completed: {results}")


if __name__ == "__main__":
    asyncio.run(main())
