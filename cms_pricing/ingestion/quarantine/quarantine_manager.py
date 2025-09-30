"""
DIS-Compliant Quarantine Manager
Following Data Ingestion Standard PRD v1.0

This module provides quarantine handling for rejected records with
proper error tracking, metadata, and recovery capabilities.
"""

import pandas as pd
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import structlog

logger = structlog.get_logger()


@dataclass
class QuarantineRecord:
    """Individual quarantined record with error details"""
    record_id: str
    error_code: str
    error_message: str
    error_details: Dict[str, Any]
    original_data: Dict[str, Any]
    quarantine_timestamp: datetime
    validation_rule: str
    severity: str  # "error", "warning", "info"
    recovery_action: Optional[str] = None


@dataclass
class QuarantineBatch:
    """Batch of quarantined records"""
    batch_id: str
    dataset_name: str
    quarantine_timestamp: datetime
    total_records: int
    quarantined_records: int
    quarantine_rate: float
    error_summary: Dict[str, int]
    records: List[QuarantineRecord]


class QuarantineManager:
    """
    Manages quarantine operations for rejected records following DIS standards.
    
    Provides capabilities for:
    - Quarantining rejected records with detailed error information
    - Tracking quarantine statistics and trends
    - Supporting record recovery and reprocessing
    - Generating quarantine reports for monitoring
    """
    
    def __init__(self, quarantine_dir: str = "cms_pricing/ingestion/quarantine"):
        self.quarantine_dir = Path(quarantine_dir)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        self.tool_version = "1.0.0"
    
    def quarantine_records(
        self,
        rejected_df: pd.DataFrame,
        error_details: List[Dict[str, Any]],
        dataset_name: str,
        batch_id: str,
        validation_rule: str = "unknown"
    ) -> QuarantineBatch:
        """
        Quarantine rejected records with detailed error information.
        
        Args:
            rejected_df: DataFrame containing rejected records
            error_details: List of error details for each record
            dataset_name: Name of the dataset
            batch_id: Batch identifier
            validation_rule: Name of the validation rule that caused rejection
            
        Returns:
            QuarantineBatch with quarantined records
        """
        
        quarantine_timestamp = datetime.utcnow()
        quarantined_records = []
        
        # Create quarantine records for each rejected row
        for idx, (_, row) in enumerate(rejected_df.iterrows()):
            error_detail = error_details[idx] if idx < len(error_details) else {}
            
            record = QuarantineRecord(
                record_id=f"{batch_id}_{idx}",
                error_code=error_detail.get("error_code", "VALIDATION_ERROR"),
                error_message=error_detail.get("error_message", "Record failed validation"),
                error_details=error_detail.get("details", {}),
                original_data=row.to_dict(),
                quarantine_timestamp=quarantine_timestamp,
                validation_rule=validation_rule,
                severity=error_detail.get("severity", "error"),
                recovery_action=error_detail.get("recovery_action")
            )
            
            quarantined_records.append(record)
        
        # Create quarantine batch
        quarantine_batch = QuarantineBatch(
            batch_id=batch_id,
            dataset_name=dataset_name,
            quarantine_timestamp=quarantine_timestamp,
            total_records=len(rejected_df),
            quarantined_records=len(quarantined_records),
            quarantine_rate=len(quarantined_records) / len(rejected_df) if len(rejected_df) > 0 else 0,
            error_summary=self._summarize_errors(quarantined_records),
            records=quarantined_records
        )
        
        # Save quarantine batch to disk
        self._save_quarantine_batch(quarantine_batch)
        
        logger.warning(
            f"Quarantined {len(quarantined_records)} records",
            dataset=dataset_name,
            batch_id=batch_id,
            quarantine_rate=quarantine_batch.quarantine_rate
        )
        
        return quarantine_batch
    
    def _summarize_errors(self, records: List[QuarantineRecord]) -> Dict[str, int]:
        """Summarize error codes and their counts"""
        error_summary = {}
        for record in records:
            error_code = record.error_code
            error_summary[error_code] = error_summary.get(error_code, 0) + 1
        return error_summary
    
    def _save_quarantine_batch(self, batch: QuarantineBatch) -> None:
        """Save quarantine batch to disk"""
        
        # Create dataset-specific quarantine directory
        dataset_dir = self.quarantine_dir / batch.dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        # Save batch metadata
        batch_file = dataset_dir / f"{batch.batch_id}_batch.json"
        with open(batch_file, 'w') as f:
            json.dump({
                "batch_id": batch.batch_id,
                "dataset_name": batch.dataset_name,
                "quarantine_timestamp": batch.quarantine_timestamp.isoformat(),
                "total_records": batch.total_records,
                "quarantined_records": batch.quarantined_records,
                "quarantine_rate": batch.quarantine_rate,
                "error_summary": batch.error_summary
            }, f, indent=2)
        
        # Save quarantined records as CSV
        records_file = dataset_dir / f"{batch.batch_id}_records.csv"
        records_data = []
        for record in batch.records:
            record_data = {
                "record_id": record.record_id,
                "error_code": record.error_code,
                "error_message": record.error_message,
                "validation_rule": record.validation_rule,
                "severity": record.severity,
                "quarantine_timestamp": record.quarantine_timestamp.isoformat(),
                "recovery_action": record.recovery_action,
                **record.original_data
            }
            records_data.append(record_data)
        
        records_df = pd.DataFrame(records_data)
        records_df.to_csv(records_file, index=False)
        
        logger.info(f"Saved quarantine batch: {batch_file}")
    
    def get_quarantine_stats(self, dataset_name: str, days: int = 30) -> Dict[str, Any]:
        """Get quarantine statistics for a dataset"""
        
        dataset_dir = self.quarantine_dir / dataset_name
        if not dataset_dir.exists():
            return {"error": f"No quarantine data found for {dataset_name}"}
        
        # Find batch files from the last N days
        cutoff_date = datetime.now().timestamp() - (days * 24 * 60 * 60)
        batch_files = []
        
        for batch_file in dataset_dir.glob("*_batch.json"):
            if batch_file.stat().st_mtime > cutoff_date:
                batch_files.append(batch_file)
        
        if not batch_files:
            return {"error": f"No quarantine data found for {dataset_name} in last {days} days"}
        
        # Aggregate statistics
        total_batches = len(batch_files)
        total_quarantined = 0
        total_records = 0
        error_codes = {}
        
        for batch_file in batch_files:
            with open(batch_file, 'r') as f:
                batch_data = json.load(f)
            
            total_quarantined += batch_data["quarantined_records"]
            total_records += batch_data["total_records"]
            
            for error_code, count in batch_data["error_summary"].items():
                error_codes[error_code] = error_codes.get(error_code, 0) + count
        
        return {
            "dataset_name": dataset_name,
            "period_days": days,
            "total_batches": total_batches,
            "total_quarantined_records": total_quarantined,
            "total_processed_records": total_records,
            "overall_quarantine_rate": total_quarantined / total_records if total_records > 0 else 0,
            "error_code_summary": error_codes
        }
    
    def list_quarantine_batches(self, dataset_name: str) -> List[Dict[str, Any]]:
        """List all quarantine batches for a dataset"""
        
        dataset_dir = self.quarantine_dir / dataset_name
        if not dataset_dir.exists():
            return []
        
        batches = []
        for batch_file in dataset_dir.glob("*_batch.json"):
            with open(batch_file, 'r') as f:
                batch_data = json.load(f)
            batches.append(batch_data)
        
        # Sort by quarantine timestamp (newest first)
        batches.sort(key=lambda x: x["quarantine_timestamp"], reverse=True)
        
        return batches
    
    def recover_records(
        self, 
        batch_id: str, 
        dataset_name: str,
        recovery_action: str = "reprocess"
    ) -> Dict[str, Any]:
        """
        Recover quarantined records for reprocessing.
        
        Args:
            batch_id: Batch identifier to recover
            dataset_name: Dataset name
            recovery_action: Action to take ("reprocess", "ignore", "manual")
            
        Returns:
            Recovery result information
        """
        
        dataset_dir = self.quarantine_dir / dataset_name
        records_file = dataset_dir / f"{batch_id}_records.csv"
        
        if not records_file.exists():
            return {"error": f"No quarantine records found for batch {batch_id}"}
        
        # Load quarantined records
        records_df = pd.read_csv(records_file)
        
        # Filter records that can be recovered
        recoverable_records = records_df[
            (records_df["recovery_action"] == recovery_action) |
            (records_df["recovery_action"].isna())
        ]
        
        if len(recoverable_records) == 0:
            return {"error": f"No recoverable records found for batch {batch_id}"}
        
        # Remove quarantine metadata columns
        data_columns = [col for col in recoverable_records.columns 
                       if not col.startswith(("record_id", "error_", "validation_", "quarantine_", "recovery_", "severity"))]
        
        recovered_data = recoverable_records[data_columns]
        
        # Save recovered data
        recovery_file = dataset_dir / f"{batch_id}_recovered.csv"
        recovered_data.to_csv(recovery_file, index=False)
        
        logger.info(f"Recovered {len(recovered_data)} records from batch {batch_id}")
        
        return {
            "batch_id": batch_id,
            "dataset_name": dataset_name,
            "recovered_records": len(recovered_data),
            "recovery_file": str(recovery_file),
            "recovery_action": recovery_action
        }
    
    def cleanup_old_quarantine(self, dataset_name: str, days: int = 90) -> Dict[str, Any]:
        """Clean up old quarantine data"""
        
        dataset_dir = self.quarantine_dir / dataset_name
        if not dataset_dir.exists():
            return {"error": f"No quarantine data found for {dataset_name}"}
        
        cutoff_date = datetime.now().timestamp() - (days * 24 * 60 * 60)
        cleaned_files = []
        
        for file_path in dataset_dir.glob("*"):
            if file_path.stat().st_mtime < cutoff_date:
                file_path.unlink()
                cleaned_files.append(str(file_path))
        
        logger.info(f"Cleaned up {len(cleaned_files)} old quarantine files")
        
        return {
            "dataset_name": dataset_name,
            "cleanup_days": days,
            "cleaned_files": len(cleaned_files),
            "file_paths": cleaned_files
        }
