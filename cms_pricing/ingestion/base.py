"""Base classes for data ingestion"""

import hashlib
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Dict, Any, Optional, List
import pandas as pd
import structlog

logger = structlog.get_logger()


class BaseIngester(ABC):
    """Base class for all data ingesters"""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.dataset_id = self.get_dataset_id()
        self.manifest = {
            "dataset_id": self.dataset_id,
            "ingestion_timestamp": datetime.utcnow().isoformat(),
            "files": [],
            "digest": None
        }
    
    @abstractmethod
    def get_dataset_id(self) -> str:
        """Get the dataset identifier"""
        pass
    
    @abstractmethod
    async def fetch_data(
        self,
        valuation_year: int,
        quarter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch raw data from source"""
        pass
    
    @abstractmethod
    def normalize_data(
        self,
        raw_data: Dict[str, Any],
        valuation_year: int,
        quarter: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
    """Normalize raw data into DataFrames"""
        pass
    
    @abstractmethod
    def validate_data(self, normalized_data: Dict[str, pd.DataFrame]) -> List[str]:
        """Validate normalized data and return warnings"""
        pass
    
    async def ingest(
        self,
        valuation_year: int,
        quarter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Main ingestion process"""
        
        logger.info(
            "Starting ingestion",
            dataset_id=self.dataset_id,
            year=valuation_year,
            quarter=quarter
        )
        
        try:
            # Create output directory
            timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
            build_id = f"{timestamp}_{self.dataset_id}"
            dataset_dir = os.path.join(self.output_dir, self.dataset_id, build_id)
            os.makedirs(dataset_dir, exist_ok=True)
            
            raw_dir = os.path.join(dataset_dir, "raw")
            normalized_dir = os.path.join(dataset_dir, "normalized")
            os.makedirs(raw_dir, exist_ok=True)
            os.makedirs(normalized_dir, exist_ok=True)
            
            # Fetch raw data
            raw_data = await self.fetch_data(valuation_year, quarter)
            
            # Save raw data
            for filename, content in raw_data.items():
                raw_path = os.path.join(raw_dir, filename)
                if isinstance(content, str):
                    with open(raw_path, 'w') as f:
                        f.write(content)
                else:
                    content.to_csv(raw_path, index=False)
                
                # Calculate digest
                digest = self._calculate_digest(raw_path)
                self.manifest["files"].append({
                    "filename": filename,
                    "path": f"raw/{filename}",
                    "sha256": digest,
                    "size_bytes": os.path.getsize(raw_path)
                })
            
            # Normalize data
            normalized_data = self.normalize_data(raw_data, valuation_year, quarter)
            
            # Validate data
            warnings = self.validate_data(normalized_data)
            if warnings:
                logger.warning(
                    "Data validation warnings",
                    dataset_id=self.dataset_id,
                    warnings=warnings
                )
            
            # Save normalized data
            for table_name, df in normalized_data.items():
                normalized_path = os.path.join(normalized_dir, f"{table_name}.parquet")
                df.to_parquet(normalized_path, index=False)
                
                # Calculate digest
                digest = self._calculate_digest(normalized_path)
                self.manifest["files"].append({
                    "filename": f"{table_name}.parquet",
                    "path": f"normalized/{table_name}.parquet",
                    "sha256": digest,
                    "size_bytes": os.path.getsize(normalized_path),
                    "rows": len(df),
                    "columns": list(df.columns)
                })
            
            # Calculate dataset digest
            dataset_digest = self._calculate_dataset_digest()
            self.manifest["digest"] = dataset_digest
            
            # Save manifest
            manifest_path = os.path.join(dataset_dir, "manifest.json")
            with open(manifest_path, 'w') as f:
                json.dump(self.manifest, f, indent=2)
            
            logger.info(
                "Ingestion completed successfully",
                dataset_id=self.dataset_id,
                build_id=build_id,
                dataset_digest=dataset_digest,
                files_processed=len(self.manifest["files"])
            )
            
            return {
                "dataset_id": self.dataset_id,
                "build_id": build_id,
                "digest": dataset_digest,
                "manifest": self.manifest,
                "warnings": warnings
            }
            
        except Exception as e:
            logger.error(
                "Ingestion failed",
                dataset_id=self.dataset_id,
                error=str(e),
                exc_info=True
            )
            raise
    
    def _calculate_digest(self, file_path: str) -> str:
        """Calculate SHA256 digest of a file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def _calculate_dataset_digest(self) -> str:
        """Calculate dataset digest from file digests"""
        file_digests = [f["sha256"] for f in self.manifest["files"]]
        combined = "".join(sorted(file_digests))
        return hashlib.sha256(combined.encode()).hexdigest()
    
    def _get_effective_dates(
        self,
        valuation_year: int,
        quarter: Optional[str] = None
    ) -> tuple[date, Optional[date]]:
        """Get effective date range for the dataset"""
        
        if quarter:
            # Quarterly dataset
            quarter_start_month = (int(quarter) - 1) * 3 + 1
            effective_from = date(valuation_year, quarter_start_month, 1)
            
            # End of quarter
            if quarter == "1":
                effective_to = date(valuation_year, 3, 31)
            elif quarter == "2":
                effective_to = date(valuation_year, 6, 30)
            elif quarter == "3":
                effective_to = date(valuation_year, 9, 30)
            else:  # quarter == "4"
                effective_to = date(valuation_year, 12, 31)
        else:
            # Annual dataset
            effective_from = date(valuation_year, 1, 1)
            effective_to = date(valuation_year, 12, 31)
        
        return effective_from, effective_to
