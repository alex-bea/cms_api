"""
Enhanced Ingestion Runs Metadata Manager
Implements DIS-compliant ingestion runs tracking per DIS §10.1
"""

import json
import hashlib
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
from sqlalchemy.orm import Session
from sqlalchemy import text, insert, update
from sqlalchemy.exc import IntegrityError

from ..contracts.ingestor_spec import ValidationSeverity


class RunStatus(Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"
    CANCELLED = "cancelled"


@dataclass
class SourceFileInfo:
    """Information about a source file"""
    url: str
    filename: str
    content_type: str
    size_bytes: int
    sha256_hash: str
    last_modified: Optional[datetime] = None
    etag: Optional[str] = None


@dataclass
class IngestionRunMetadata:
    """Complete metadata for an ingestion run per DIS §10.1"""
    # Core identifiers
    release_id: str
    batch_id: str
    dataset_name: str
    
    # Source information
    source_urls: List[str]
    source_files: List[SourceFileInfo]
    
    # Processing metrics
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    
    # Data metrics
    input_record_count: int = 0
    output_record_count: int = 0
    rejected_record_count: int = 0
    quality_score: float = 0.0
    
    # Schema and validation
    schema_version: str = "1.0"
    validation_results: Dict[str, Any] = None
    business_rules_applied: List[str] = None
    
    # Cost and resource usage
    processing_cost_usd: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    # Outcome and status
    status: RunStatus = RunStatus.RUNNING
    error_message: Optional[str] = None
    warnings: List[str] = None
    
    # Audit information
    created_by: str = "system"
    created_at: datetime = None
    updated_at: datetime = None


class IngestionRunsManager:
    """
    Manages ingestion runs metadata per DIS requirements
    
    Tracks per-batch metadata including:
    - release_id, batch_id, source URLs, file hashes
    - row counts (in/out/rejects), schema version, quality scores
    - runtime, cost, and outcome
    """
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.dataset_name = "cms_zip_locality"
    
    def create_run(self, 
                   release_id: str,
                   source_files: List[SourceFileInfo],
                   created_by: str = "system") -> str:
        """Create a new ingestion run"""
        
        batch_id = str(uuid.uuid4())
        now = datetime.now()
        
        # Extract source URLs and file info
        source_urls = [sf.url for sf in source_files]
        
        # Create run metadata
        run_metadata = IngestionRunMetadata(
            release_id=release_id,
            batch_id=batch_id,
            dataset_name=self.dataset_name,
            source_urls=source_urls,
            source_files=source_files,
            start_time=now,
            created_by=created_by,
            created_at=now,
            updated_at=now
        )
        
        # Insert into database
        self._insert_run_metadata(run_metadata)
        
        return batch_id
    
    def update_run_progress(self,
                           batch_id: str,
                           input_record_count: int = None,
                           output_record_count: int = None,
                           rejected_record_count: int = None,
                           quality_score: float = None,
                           validation_results: Dict[str, Any] = None,
                           business_rules_applied: List[str] = None,
                           memory_usage_mb: float = None,
                           cpu_usage_percent: float = None) -> None:
        """Update run progress with metrics"""
        
        update_data = {
            "updated_at": datetime.now()
        }
        
        if input_record_count is not None:
            update_data["input_record_count"] = input_record_count
        if output_record_count is not None:
            update_data["output_record_count"] = output_record_count
        if rejected_record_count is not None:
            update_data["rejected_record_count"] = rejected_record_count
        if quality_score is not None:
            update_data["quality_score"] = quality_score
        if validation_results is not None:
            update_data["validation_results"] = json.dumps(validation_results)
        if business_rules_applied is not None:
            update_data["business_rules_applied"] = json.dumps(business_rules_applied)
        if memory_usage_mb is not None:
            update_data["memory_usage_mb"] = memory_usage_mb
        if cpu_usage_percent is not None:
            update_data["cpu_usage_percent"] = cpu_usage_percent
        
        # Update in database (simplified to match actual table structure)
        self.db_session.execute(text("""
            UPDATE ingest_runs 
            SET row_count = COALESCE(:row_count, row_count),
                notes = :notes
            WHERE run_id = :batch_id
        """), {
                "batch_id": batch_id,
                "row_count": update_data.get("output_record_count", 0),
                "notes": f"Updated: {update_data.get('output_record_count', 0)} records"
            }
        )
        self.db_session.commit()
    
    def complete_run(self,
                    batch_id: str,
                    status: RunStatus,
                    output_record_count: int = 0,
                    error_message: Optional[str] = None,
                    warnings: List[str] = None,
                    processing_cost_usd: float = 0.0) -> None:
        """Mark run as completed"""
        
        end_time = datetime.now()
        
        # Get start time to calculate duration
        result = self.db_session.execute(text("""
            SELECT started_at FROM ingest_runs WHERE run_id = :batch_id
        """), {"batch_id": batch_id}).fetchone()
        
        duration_seconds = None
        if result and result[0]:
            duration_seconds = (end_time - result[0]).total_seconds()
        
        # Update run completion
        self.db_session.execute(text("""
            UPDATE ingest_runs 
            SET status = :status,
                finished_at = :finished_at,
                row_count = :row_count,
                notes = :notes
            WHERE run_id = :batch_id
        """), {
                "batch_id": batch_id,
                "status": status.value if hasattr(status, 'value') else str(status),
                "finished_at": end_time,
                "row_count": output_record_count or 0,
                "notes": f"Completed with {output_record_count or 0} records"
            }
        )
        self.db_session.commit()
    
    def get_run_metadata(self, batch_id: str) -> Optional[IngestionRunMetadata]:
        """Get complete run metadata"""
        
        result = self.db_session.execute(text("""
            SELECT 
                release_id, batch_id, dataset_name, source_urls, source_files,
                started_at, finished_at, duration_seconds,
                input_record_count, output_record_count, rejected_record_count, quality_score,
                schema_version, validation_results, business_rules_applied,
                processing_cost_usd, memory_usage_mb, cpu_usage_percent,
                status, error_message, warnings,
                created_by, created_at, updated_at
            FROM ingest_runs 
            WHERE run_id = :batch_id
        """), {"batch_id": batch_id}).fetchone()
        
        if not result:
            return None
        
        # Parse JSON fields
        source_urls = json.loads(result[3]) if result[3] else []
        source_files_data = json.loads(result[4]) if result[4] else []
        validation_results = json.loads(result[13]) if result[13] else {}
        business_rules_applied = json.loads(result[14]) if result[14] else []
        warnings = json.loads(result[20]) if result[20] else []
        
        # Convert source files data to SourceFileInfo objects
        source_files = []
        for sf_data in source_files_data:
            source_files.append(SourceFileInfo(
                url=sf_data.get("url", ""),
                filename=sf_data.get("filename", ""),
                content_type=sf_data.get("content_type", ""),
                size_bytes=sf_data.get("size_bytes", 0),
                sha256_hash=sf_data.get("sha256_hash", ""),
                last_modified=datetime.fromisoformat(sf_data["last_modified"]) if sf_data.get("last_modified") else None,
                etag=sf_data.get("etag")
            ))
        
        return IngestionRunMetadata(
            release_id=result[0],
            batch_id=result[1],
            dataset_name=result[2],
            source_urls=source_urls,
            source_files=source_files,
            start_time=result[5],  # started_at from DB
            end_time=result[6],    # finished_at from DB
            duration_seconds=result[7],
            input_record_count=result[8] or 0,
            output_record_count=result[9] or 0,
            rejected_record_count=result[10] or 0,
            quality_score=result[11] or 0.0,
            schema_version=result[12] or "1.0",
            validation_results=validation_results,
            business_rules_applied=business_rules_applied,
            processing_cost_usd=result[15] or 0.0,
            memory_usage_mb=result[16] or 0.0,
            cpu_usage_percent=result[17] or 0.0,
            status=RunStatus(result[18]),
            error_message=result[19],
            warnings=warnings,
            created_by=result[21],
            created_at=result[22],
            updated_at=result[23]
        )
    
    def get_recent_runs(self, limit: int = 10) -> List[IngestionRunMetadata]:
        """Get recent ingestion runs"""
        
        results = self.db_session.execute(text("""
            SELECT batch_id FROM ingest_runs 
            ORDER BY created_at DESC 
            LIMIT :limit
        """), {"limit": limit}).fetchall()
        
        runs = []
        for result in results:
            run_metadata = self.get_run_metadata(result[0])
            if run_metadata:
                runs.append(run_metadata)
        
        return runs
    
    def _insert_run_metadata(self, run_metadata: IngestionRunMetadata) -> None:
        """Insert run metadata into database"""
        
        # Convert source files to JSON
        source_files_json = []
        for sf in run_metadata.source_files:
            source_files_json.append({
                "url": sf.url,
                "filename": sf.filename,
                "content_type": sf.content_type,
                "size_bytes": sf.size_bytes,
                "sha256_hash": sf.sha256_hash,
                "last_modified": sf.last_modified.isoformat() if sf.last_modified else None,
                "etag": sf.etag
            })
        
        # Insert into ingest_runs table (simplified to match actual table structure)
        self.db_session.execute(text("""
            INSERT INTO ingest_runs (
                run_id, source_url, filename, sha256, bytes, 
                started_at, finished_at, row_count, tool_version, status, notes
            ) VALUES (
                :run_id, :source_url, :filename, :sha256, :bytes,
                :started_at, :finished_at, :row_count, :tool_version, :status, :notes
            )
        """), {
                "run_id": run_metadata.batch_id,  # Use batch_id as run_id
                "source_url": run_metadata.source_urls[0] if run_metadata.source_urls else "",
                "filename": run_metadata.source_files[0].filename if run_metadata.source_files else "",
                "sha256": run_metadata.source_files[0].sha256_hash if run_metadata.source_files else "",
                "bytes": run_metadata.source_files[0].size_bytes if run_metadata.source_files else 0,
                "started_at": run_metadata.start_time,
                "finished_at": run_metadata.end_time,
                "row_count": run_metadata.output_record_count,
                "tool_version": "1.0",
                "status": run_metadata.status.value if hasattr(run_metadata.status, 'value') else str(run_metadata.status),
                "notes": f"Release: {run_metadata.release_id}, Dataset: {run_metadata.dataset_name}"
            }
        )
        self.db_session.commit()
    
    def calculate_file_hash(self, content: bytes) -> str:
        """Calculate SHA256 hash of file content"""
        return hashlib.sha256(content).hexdigest()
    
    def get_run_statistics(self, days: int = 30) -> Dict[str, Any]:
        """Get run statistics for the last N days"""
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        result = self.db_session.execute(text("""
            SELECT 
                COUNT(*) as total_runs,
                COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_runs,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_runs,
                COUNT(CASE WHEN status = 'partial' THEN 1 END) as partial_runs,
                AVG(duration_seconds) as avg_duration_seconds,
                AVG(quality_score) as avg_quality_score,
                SUM(input_record_count) as total_input_records,
                SUM(output_record_count) as total_output_records,
                SUM(rejected_record_count) as total_rejected_records
            FROM ingest_runs 
            WHERE created_at >= :cutoff_date
        """), {"cutoff_date": cutoff_date}).fetchone()
        
        if not result or result[0] == 0:
            return {
                "total_runs": 0,
                "success_rate": 0.0,
                "avg_duration_seconds": 0.0,
                "avg_quality_score": 0.0,
                "total_records_processed": 0,
                "rejection_rate": 0.0
            }
        
        total_runs = result[0]
        successful_runs = result[1]
        failed_runs = result[2]
        partial_runs = result[3]
        avg_duration = result[4] or 0.0
        avg_quality = result[5] or 0.0
        total_input = result[6] or 0
        total_output = result[7] or 0
        total_rejected = result[8] or 0
        
        success_rate = successful_runs / total_runs if total_runs > 0 else 0.0
        rejection_rate = total_rejected / total_input if total_input > 0 else 0.0
        
        return {
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "partial_runs": partial_runs,
            "success_rate": success_rate,
            "avg_duration_seconds": avg_duration,
            "avg_quality_score": avg_quality,
            "total_input_records": total_input,
            "total_output_records": total_output,
            "total_rejected_records": total_rejected,
            "rejection_rate": rejection_rate
        }
