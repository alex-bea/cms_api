"""
DIS-Compliant IngestorSpec Interface
Following Data Ingestion Standard PRD v1.0

This module defines the minimal interface that all ingestors must implement
to be compliant with the DIS standard.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Optional, Any, Dict
from datetime import datetime


class DataClass(Enum):
    """Data classification levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class ReleaseCadence(Enum):
    """Release cadence types"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"
    ON_DEMAND = "on_demand"


class ValidationSeverity(Enum):
    """Validation severity levels"""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


@dataclass
class SourceFile:
    """Represents a source file to be ingested"""
    url: str
    filename: str
    content_type: str
    expected_size_bytes: Optional[int] = None
    last_modified: Optional[datetime] = None
    etag: Optional[str] = None
    checksum: Optional[str] = None


@dataclass
class RawBatch:
    """Raw data batch from source"""
    source_files: List[SourceFile]
    raw_content: Dict[str, bytes]
    metadata: Dict[str, Any]


@dataclass
class AdaptedBatch:
    """Data after header/type adaptation"""
    dataframes: Dict[str, Any]  # pandas DataFrames
    schema_contract: Dict[str, Any]
    metadata: Dict[str, Any]


@dataclass
class ValidationRule:
    """Validation rule definition"""
    name: str
    description: str
    validator_func: Callable[[Any], bool]
    severity: str  # "error", "warning", "info"
    threshold: Optional[float] = None


@dataclass
class StageFrame:
    """Staged data frame"""
    data: Any  # pandas DataFrame or similar
    schema: Dict[str, Any]
    metadata: Dict[str, Any]
    quality_metrics: Dict[str, Any]


@dataclass
class RefData:
    """Reference data for enrichment"""
    tables: Dict[str, Any]
    metadata: Dict[str, Any]


@dataclass
class OutputSpec:
    """Output specification"""
    table_name: str
    partition_columns: List[str]
    output_format: str  # "parquet", "csv", "json"
    compression: str = "snappy"
    schema_evolution: bool = True


@dataclass
class SlaSpec:
    """SLA specification"""
    max_processing_time_hours: float
    freshness_alert_hours: float
    quality_threshold: float
    availability_target: float


class IngestorSpec(ABC):
    """
    Minimal interface that all DIS-compliant ingestors must implement.
    
    This interface ensures consistency across all data ingestion pipelines
    and enables standardized monitoring, validation, and orchestration.
    """
    
    @property
    @abstractmethod
    def dataset_name(self) -> str:
        """Unique dataset identifier (e.g., 'cms_zip_locality')"""
        pass
    
    @property
    @abstractmethod
    def release_cadence(self) -> ReleaseCadence:
        """Expected release cadence from source"""
        pass
    
    @property
    @abstractmethod
    def contract_schema_ref(self) -> str:
        """Reference to schema contract (e.g., 'contracts/cms_zip_locality_v1.avsc')"""
        pass
    
    @property
    @abstractmethod
    def discovery(self) -> Callable[[], List[SourceFile]]:
        """Function to discover source files"""
        pass
    
    @property
    @abstractmethod
    def adapter(self) -> Callable[[RawBatch], AdaptedBatch]:
        """Function to adapt raw data (header mapping, type conversion)"""
        pass
    
    @property
    @abstractmethod
    def validators(self) -> List[ValidationRule]:
        """List of validation rules to apply"""
        pass
    
    @property
    @abstractmethod
    def enricher(self) -> Callable[[StageFrame, RefData], StageFrame]:
        """Function to enrich data with reference tables"""
        pass
    
    @property
    @abstractmethod
    def outputs(self) -> OutputSpec:
        """Output specification"""
        pass
    
    @property
    @abstractmethod
    def slas(self) -> SlaSpec:
        """SLA specification"""
        pass
    
    @property
    @abstractmethod
    def classification(self) -> DataClass:
        """Data classification level"""
        pass
    
    @abstractmethod
    async def ingest(self, release_id: str, batch_id: str) -> Dict[str, Any]:
        """
        Main ingestion method following DIS Land → Validate → Normalize → Enrich → Publish
        
        Args:
            release_id: Unique identifier for this release
            batch_id: Unique identifier for this batch execution
            
        Returns:
            Dictionary with ingestion results, metrics, and metadata
        """
        pass


class BaseDISIngestor(IngestorSpec):
    """
    Base implementation of IngestorSpec with common DIS functionality.
    
    Provides default implementations for common patterns while allowing
    subclasses to override specific behavior.
    """
    
    def __init__(self, output_dir: str, db_session: Any = None):
        self.output_dir = output_dir
        self.db_session = db_session
        self.tool_version = "1.0.0"
        self.dis_version = "1.0"
    
    @property
    def discovery(self) -> Callable[[], List[SourceFile]]:
        """Default discovery implementation - subclasses should override"""
        return lambda: []
    
    async def ingest(self, release_id: str, batch_id: str) -> Dict[str, Any]:
        """
        Default DIS-compliant ingestion pipeline implementation.
        
        Subclasses can override individual stages as needed.
        """
        from .pipeline import DISPipeline
        
        pipeline = DISPipeline(
            ingestor=self,
            output_dir=self.output_dir,
            db_session=self.db_session
        )
        
        return await pipeline.execute(release_id, batch_id)
    
    def _create_manifest(self, release_id: str, batch_id: str, source_files: List[SourceFile]) -> Dict[str, Any]:
        """Create manifest.json following DIS requirements"""
        return {
            "release_id": release_id,
            "batch_id": batch_id,
            "dataset_name": self.dataset_name,
            "dis_version": self.dis_version,
            "tool_version": self.tool_version,
            "fetched_at": datetime.utcnow().isoformat(),
            "source_files": [
                {
                    "url": sf.url,
                    "filename": sf.filename,
                    "content_type": sf.content_type,
                    "size_bytes": sf.expected_size_bytes,
                    "last_modified": sf.last_modified.isoformat() if sf.last_modified else None,
                    "etag": sf.etag,
                    "checksum": sf.checksum
                }
                for sf in source_files
            ],
            "license": {
                "name": "CMS Open Data",
                "url": "https://www.cms.gov/About-CMS/Agency-Information/Aboutwebsite/Privacy-Policy",
                "attribution_required": True
            }
        }
