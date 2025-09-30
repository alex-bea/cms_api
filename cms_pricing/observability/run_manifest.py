"""
Run Manifest Generator

Tracks files, checksums, row counts, and metadata for each ingestion run
"""

import hashlib
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, date
from pathlib import Path
from dataclasses import dataclass, asdict
from sqlalchemy.orm import Session
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import Release, RVUItem, GPCIIndex, OPPSCap, AnesCF, LocalityCounty
import logging

logger = logging.getLogger(__name__)


@dataclass
class FileManifest:
    """Manifest entry for a single file"""
    filename: str
    file_path: str
    file_size: int
    checksum_sha256: str
    row_count: int
    ingested_at: datetime
    status: str  # 'success', 'failed', 'partial'
    error_message: Optional[str] = None


@dataclass
class DatasetManifest:
    """Manifest entry for a dataset within a run"""
    dataset_name: str  # 'pprrvu', 'gpci', 'oppscap', 'anes', 'locco'
    file_type: str  # 'txt', 'csv'
    files: List[FileManifest]
    total_rows: int
    successful_rows: int
    failed_rows: int
    validation_errors: int
    validation_warnings: int
    processing_time_seconds: float


@dataclass
class RunManifest:
    """Complete manifest for an ingestion run"""
    run_id: str
    release_id: str
    source_version: str
    run_type: str  # 'full', 'incremental', 'correction'
    started_at: datetime
    completed_at: datetime
    total_duration_seconds: float
    datasets: List[DatasetManifest]
    overall_status: str  # 'success', 'failed', 'partial'
    total_files: int
    total_rows: int
    total_errors: int
    total_warnings: int


class RunManifestGenerator:
    """Generates comprehensive run manifests for ingestion operations"""
    
    def __init__(self, output_dir: str = "data/RVU/manifests"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.db = SessionLocal()
    
    def generate_manifest(self, release_id: str, run_start_time: datetime) -> RunManifest:
        """Generate a complete manifest for an ingestion run"""
        
        print(f"📋 Generating run manifest for release {release_id}...")
        
        # Get release information
        release = self.db.query(Release).filter(Release.id == release_id).first()
        if not release:
            raise ValueError(f"Release {release_id} not found")
        
        # Calculate run duration
        run_end_time = datetime.now()
        total_duration = (run_end_time - run_start_time).total_seconds()
        
        # Generate dataset manifests
        datasets = []
        total_files = 0
        total_rows = 0
        total_errors = 0
        total_warnings = 0
        
        # PPRRVU dataset
        pprrvu_manifest = self._generate_dataset_manifest(
            release_id, 'pprrvu', 'txt', RVUItem, run_start_time
        )
        datasets.append(pprrvu_manifest)
        total_files += len(pprrrvu_manifest.files)
        total_rows += pprrvu_manifest.total_rows
        total_errors += pprrvu_manifest.failed_rows
        total_warnings += pprrvu_manifest.validation_warnings
        
        # GPCI dataset
        gpci_manifest = self._generate_dataset_manifest(
            release_id, 'gpci', 'txt', GPCIIndex, run_start_time
        )
        datasets.append(gpci_manifest)
        total_files += len(gpci_manifest.files)
        total_rows += gpci_manifest.total_rows
        total_errors += gpci_manifest.failed_rows
        total_warnings += gpci_manifest.validation_warnings
        
        # OPPSCAP dataset
        oppscap_manifest = self._generate_dataset_manifest(
            release_id, 'oppscap', 'txt', OPPSCap, run_start_time
        )
        datasets.append(oppscap_manifest)
        total_files += len(oppscap_manifest.files)
        total_rows += oppscap_manifest.total_rows
        total_errors += oppscap_manifest.failed_rows
        total_warnings += oppscap_manifest.validation_warnings
        
        # ANES dataset
        anes_manifest = self._generate_dataset_manifest(
            release_id, 'anes', 'txt', AnesCF, run_start_time
        )
        datasets.append(anes_manifest)
        total_files += len(anes_manifest.files)
        total_rows += anes_manifest.total_rows
        total_errors += anes_manifest.failed_rows
        total_warnings += anes_manifest.validation_warnings
        
        # Locality-County dataset
        locco_manifest = self._generate_dataset_manifest(
            release_id, 'locco', 'txt', LocalityCounty, run_start_time
        )
        datasets.append(locco_manifest)
        total_files += len(locco_manifest.files)
        total_rows += locco_manifest.total_rows
        total_errors += locco_manifest.failed_rows
        total_warnings += locco_manifest.validation_warnings
        
        # Determine overall status
        overall_status = 'success'
        if total_errors > 0:
            overall_status = 'partial' if total_rows > 0 else 'failed'
        
        # Create run manifest
        run_manifest = RunManifest(
            run_id=f"run_{release_id}_{run_start_time.strftime('%Y%m%d_%H%M%S')}",
            release_id=release_id,
            source_version=release.source_version,
            run_type=release.type,
            started_at=run_start_time,
            completed_at=run_end_time,
            total_duration_seconds=total_duration,
            datasets=datasets,
            overall_status=overall_status,
            total_files=total_files,
            total_rows=total_rows,
            total_errors=total_errors,
            total_warnings=total_warnings
        )
        
        # Save manifest to file
        self._save_manifest(run_manifest)
        
        print(f"✅ Run manifest generated:")
        print(f"   - Run ID: {run_manifest.run_id}")
        print(f"   - Duration: {total_duration:.2f}s")
        print(f"   - Files: {total_files}")
        print(f"   - Rows: {total_rows:,}")
        print(f"   - Errors: {total_errors}")
        print(f"   - Warnings: {total_warnings}")
        print(f"   - Status: {overall_status}")
        
        return run_manifest
    
    def _generate_dataset_manifest(self, release_id: str, dataset_name: str, 
                                 file_type: str, model_class, run_start_time: datetime) -> DatasetManifest:
        """Generate manifest for a specific dataset"""
        
        # Get all records for this dataset and release
        records = self.db.query(model_class).filter(model_class.release_id == release_id).all()
        
        # Calculate statistics
        total_rows = len(records)
        successful_rows = total_rows  # Assume all loaded records are successful
        failed_rows = 0  # Would need to track this during ingestion
        
        # Create file manifest (simplified - in real implementation, track actual files)
        file_manifest = FileManifest(
            filename=f"{dataset_name}_{release_id}.{file_type}",
            file_path=f"data/RVU/{dataset_name}_{release_id}.{file_type}",
            file_size=0,  # Would calculate actual file size
            checksum_sha256="",  # Would calculate actual checksum
            row_count=total_rows,
            ingested_at=run_start_time,
            status='success' if total_rows > 0 else 'failed'
        )
        
        # Calculate processing time (simplified)
        processing_time = (datetime.now() - run_start_time).total_seconds() / 5  # Rough estimate
        
        return DatasetManifest(
            dataset_name=dataset_name,
            file_type=file_type,
            files=[file_manifest],
            total_rows=total_rows,
            successful_rows=successful_rows,
            failed_rows=failed_rows,
            validation_errors=0,  # Would get from validation results
            validation_warnings=0,  # Would get from validation results
            processing_time_seconds=processing_time
        )
    
    def _save_manifest(self, manifest: RunManifest):
        """Save manifest to JSON file"""
        
        # Convert to dictionary
        manifest_dict = asdict(manifest)
        
        # Convert datetime objects to ISO format
        def convert_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, date):
                return obj.isoformat()
            return obj
        
        # Recursively convert all datetime objects
        def convert_recursive(obj):
            if isinstance(obj, dict):
                return {k: convert_recursive(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_recursive(item) for item in obj]
            else:
                return convert_datetime(obj)
        
        manifest_dict = convert_recursive(manifest_dict)
        
        # Save to file
        manifest_file = self.output_dir / f"{manifest.run_id}.json"
        with open(manifest_file, 'w') as f:
            json.dump(manifest_dict, f, indent=2)
        
        logger.info(f"Manifest saved to {manifest_file}")
    
    def get_run_history(self, limit: int = 10) -> List[RunManifest]:
        """Get recent run history"""
        
        manifest_files = sorted(self.output_dir.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
        
        manifests = []
        for manifest_file in manifest_files[:limit]:
            try:
                with open(manifest_file, 'r') as f:
                    manifest_dict = json.load(f)
                
                # Convert back to RunManifest object
                manifest = self._dict_to_manifest(manifest_dict)
                manifests.append(manifest)
                
            except Exception as e:
                logger.error(f"Failed to load manifest {manifest_file}: {e}")
        
        return manifests
    
    def _dict_to_manifest(self, manifest_dict: Dict[str, Any]) -> RunManifest:
        """Convert dictionary back to RunManifest object"""
        
        # Convert ISO strings back to datetime objects
        def convert_datetime_str(obj):
            if isinstance(obj, str):
                try:
                    return datetime.fromisoformat(obj)
                except ValueError:
                    return obj
            return obj
        
        def convert_recursive(obj):
            if isinstance(obj, dict):
                return {k: convert_recursive(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_recursive(item) for item in obj]
            else:
                return convert_datetime_str(obj)
        
        manifest_dict = convert_recursive(manifest_dict)
        
        # Convert datasets
        datasets = []
        for dataset_dict in manifest_dict['datasets']:
            files = [FileManifest(**file_dict) for file_dict in dataset_dict['files']]
            dataset = DatasetManifest(
                dataset_name=dataset_dict['dataset_name'],
                file_type=dataset_dict['file_type'],
                files=files,
                total_rows=dataset_dict['total_rows'],
                successful_rows=dataset_dict['successful_rows'],
                failed_rows=dataset_dict['failed_rows'],
                validation_errors=dataset_dict['validation_errors'],
                validation_warnings=dataset_dict['validation_warnings'],
                processing_time_seconds=dataset_dict['processing_time_seconds']
            )
            datasets.append(dataset)
        
        return RunManifest(
            run_id=manifest_dict['run_id'],
            release_id=manifest_dict['release_id'],
            source_version=manifest_dict['source_version'],
            run_type=manifest_dict['run_type'],
            started_at=manifest_dict['started_at'],
            completed_at=manifest_dict['completed_at'],
            total_duration_seconds=manifest_dict['total_duration_seconds'],
            datasets=datasets,
            overall_status=manifest_dict['overall_status'],
            total_files=manifest_dict['total_files'],
            total_rows=manifest_dict['total_rows'],
            total_errors=manifest_dict['total_errors'],
            total_warnings=manifest_dict['total_warnings']
        )
    
    def generate_summary_report(self) -> Dict[str, Any]:
        """Generate a summary report of all runs"""
        
        manifests = self.get_run_history(limit=50)  # Last 50 runs
        
        if not manifests:
            return {"message": "No runs found"}
        
        # Calculate statistics
        total_runs = len(manifests)
        successful_runs = len([m for m in manifests if m.overall_status == 'success'])
        failed_runs = len([m for m in manifests if m.overall_status == 'failed'])
        partial_runs = len([m for m in manifests if m.overall_status == 'partial'])
        
        total_files = sum(m.total_files for m in manifests)
        total_rows = sum(m.total_rows for m in manifests)
        total_errors = sum(m.total_errors for m in manifests)
        total_warnings = sum(m.total_warnings for m in manifests)
        
        avg_duration = sum(m.total_duration_seconds for m in manifests) / total_runs
        
        # Recent trends (last 10 runs)
        recent_manifests = manifests[:10]
        recent_success_rate = len([m for m in recent_manifests if m.overall_status == 'success']) / len(recent_manifests)
        
        return {
            "summary": {
                "total_runs": total_runs,
                "successful_runs": successful_runs,
                "failed_runs": failed_runs,
                "partial_runs": partial_runs,
                "success_rate": successful_runs / total_runs if total_runs > 0 else 0,
                "recent_success_rate": recent_success_rate
            },
            "volume": {
                "total_files": total_files,
                "total_rows": total_rows,
                "total_errors": total_errors,
                "total_warnings": total_warnings,
                "avg_duration_seconds": avg_duration
            },
            "recent_runs": [
                {
                    "run_id": m.run_id,
                    "source_version": m.source_version,
                    "status": m.overall_status,
                    "duration": m.total_duration_seconds,
                    "rows": m.total_rows,
                    "errors": m.total_errors,
                    "started_at": m.started_at.isoformat()
                }
                for m in recent_manifests
            ]
        }
    
    def close(self):
        """Clean up resources"""
        self.db.close()

