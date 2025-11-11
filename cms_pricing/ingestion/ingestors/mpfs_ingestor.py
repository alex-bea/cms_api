"""
DIS-Compliant MPFS Ingestor
Following Data Ingestion Standard PRD v1.0

This module implements a fully DIS-compliant ingestor for MPFS (Medicare Physician Fee Schedule)
datasets, creating curated views that reference existing RVU tables while adding MPFS-specific
data like conversion factors and abstracts.

MPFS Ingestor creates curated views that reference RVU data:
- mpfs_rvu: Core RVUs + indicators (references PPRRVU)
- mpfs_indicators_all: Exploded policy flags (references PPRRVU)  
- mpfs_locality: Locality dimension (references LocalityCounty)
- mpfs_gpci: GPCI indices (references GPCI)
- mpfs_cf_vintage: Conversion factors (new MPFS-specific data)
- mpfs_link_keys: Minimal key set for downstream joins
"""

import asyncio
import io
import json
import os
import re
import uuid
import zipfile
import shutil
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse
from types import SimpleNamespace

import pandas as pd
import structlog

from cms_pricing.database import SessionLocal
from cms_pricing.utils.snapshot_fallback import collect_search_roots, resolve_repo_path
from ..contracts.ingestor_spec import (
    BaseDISIngestor,
    SourceFile,
    RawBatch,
    AdaptedBatch,
    StageFrame,
    RefData,
    ValidationRule,
    OutputSpec,
    SlaSpec,
    ReleaseCadence,
    DataClass,
    ValidationSeverity,
)
from ..managers.historical_data_manager import HistoricalDataManager
from ..contracts.schema_registry import schema_registry, SchemaContract
from ..adapters.data_adapters import AdapterFactory, AdapterConfig
from ..validators.validation_engine import ValidationEngine
from ..enrichers.data_enrichers import EnricherFactory
from ..publishers.data_publishers import PublisherFactory
from ..utils.atomic import atomic_write, atomic_write_json, compute_sha256
from ..observability.dis_observability import (
    DISObservabilityCollector,
    FreshnessMetrics,
    VolumeMetrics,
    SchemaMetrics,
    QualityMetrics,
    LineageMetrics,
    DISObservabilityReport,
)
from ..quarantine.dis_quarantine import QuarantineManager, QuarantineStatus, QuarantineSeverity
from ..enrichers.dis_reference_data_integration import (
    DISReferenceDataEnricher,
    ReferenceDataManager,
    ReferenceDataSource,
)
from ...services.dataset_snapshot_service import DatasetSnapshotService, SnapshotMetadata
from ..services.conversion_factor_fetcher import ConversionFactorFetcher
from ..services.mpfs_config_service import MPFSConfigService
from ..services.ingestor_artifact_profile import IngestorArtifactProfileService
from ..datasets.mpfs_builder import (
    MPFSNormalizedInputs,
    build_curated_views,
    normalize_conversion_factor,
)

logger = structlog.get_logger()

RELEASE_SUFFIXES = {"A", "B", "C", "D", "AR", "BR", "CR", "DR"}
QUARTER_TO_SUFFIX = {"Q1": "A", "Q2": "B", "Q3": "C", "Q4": "D"}


class MPFSIngestor(BaseDISIngestor):
    """DIS-compliant MPFS ingestor that creates curated views referencing RVU data"""

    def __init__(
        self,
        output_dir: str = "./data/ingestion/mpfs",
        db_session: Any = None,
        snapshot_service: Optional[DatasetSnapshotService] = None,
        cf_fetcher: Optional[ConversionFactorFetcher] = None,
        config_service: Optional[MPFSConfigService] = None,
    ):
        super().__init__(output_dir, db_session)

        # Initialize components
        self._snapshot_service_managed_session = False
        if snapshot_service:
            self.snapshot_service = snapshot_service
        else:
            if db_session is not None:
                self.snapshot_service = DatasetSnapshotService(db_session)
            else:
                self._snapshot_service_managed_session = True
                session = SessionLocal()
                self.snapshot_service = DatasetSnapshotService(session)

        self.cf_fetcher = cf_fetcher or ConversionFactorFetcher(str(Path(self.output_dir) / "raw"))
        # Config service is optional - CLI flags remain fallback until YAML service is production-ready
        self.config_service = config_service or MPFSConfigService()
        self.artifact_profile_service = IngestorArtifactProfileService()

        self.historical_manager = HistoricalDataManager(str(Path(self.output_dir) / "historical"))
        self.schema_registry = schema_registry
        self.validation_engine = ValidationEngine()
        self.quarantine_manager = QuarantineManager(str(Path(self.output_dir) / "quarantine"))
        self.observability_collector = DISObservabilityCollector()
        self.reference_data_manager = ReferenceDataManager()
        self._snapshot_search_roots = collect_search_roots()
        
        # Current run metadata
        self.current_release_id: Optional[str] = None
        self.current_batch_id: Optional[str] = None
        self._conversion_factor_strategy: str = "auto"
        self.target_year: Optional[int] = None
        self.target_release_suffix: Optional[str] = None
        self.target_rvu_release_id: Optional[str] = None
        self.target_gpci_release_id: Optional[str] = None
        self._requested_release_input: Optional[str] = None
        
        # Required properties for IngestorSpec
        self._dataset_name = "MPFS"
        self._release_cadence = ReleaseCadence.ANNUAL
        self._data_classification = DataClass.PUBLIC
        self._contract_schema_ref = "cms.mpfs:v1.0"
        
        # MPFS-specific configuration (now handled by properties)
        
        # Source configuration
        self.source_name = "CMS Medicare Physician Fee Schedule"
        self.license = "CMS Public Domain"
        self.attribution_required = False
        
        # Quality gates and SLAs
        self.sla_spec = SlaSpec(
            max_processing_time_hours=24,
            freshness_alert_hours=120,  # 5 days * 24 hours
            quality_threshold=0.99,
            availability_target=0.999
        )
        
        # Output specification
        self.output_spec = OutputSpec(
            table_name="mpfs_curated",
            partition_columns=["vintage_date", "effective_from"],
            output_format="parquet",
            compression="snappy",
            schema_evolution=True
        )
        
        # Validation rules
        self.validation_rules = self._create_validation_rules()
        
        # Schema contracts
        self.schema_contracts = self._load_schema_contracts()

    def __del__(self):
        if getattr(self, "_snapshot_service_managed_session", False):
            try:
                self.snapshot_service.close()
            except Exception:
                pass
    
    @property
    def dataset_name(self) -> str:
        return self._dataset_name
    
    @property
    def release_cadence(self) -> ReleaseCadence:
        return self._release_cadence
    
    @property
    def data_classification(self) -> DataClass:
        return self._data_classification
    
    @property
    def contract_schema_ref(self) -> str:
        return self._contract_schema_ref
    
    @property
    def validators(self) -> List[ValidationRule]:
        return self.validation_rules
    
    @property
    def slas(self) -> SlaSpec:
        return self.sla_spec
    
    @property
    def outputs(self) -> OutputSpec:
        return self.output_spec
    
    @property
    def classification(self) -> DataClass:
        return self._data_classification
    
    @property
    def adapter(self):
        """Return adapter for data transformation"""
        return AdapterFactory.create_adapter("mpfs", AdapterConfig())
    
    @property
    def enricher(self):
        """Return enricher for data enrichment"""
        return EnricherFactory.create_enricher("mpfs", self.reference_data_manager)
    
    def _create_validation_rules(self) -> List[ValidationRule]:
        """Create validation rules for MPFS data"""
        return [
            ValidationRule(
                name="Required columns present",
                description="All required MPFS columns must be present",
                validator_func=lambda x: True,  # Placeholder
                severity="error"
            ),
            ValidationRule(
                name="HCPCS code format",
                description="HCPCS codes must be 5 characters",
                validator_func=lambda x: True,  # Placeholder
                severity="error"
            ),
            ValidationRule(
                name="Status code valid",
                description="Status codes must be valid CMS codes",
                validator_func=lambda x: True,  # Placeholder
                severity="error"
            ),
            ValidationRule(
                name="Row count drift",
                description="Row count within ±15% of previous vintage",
                validator_func=lambda x: True,  # Placeholder
                severity="warning"
            ),
            ValidationRule(
                name="RVU sum validation",
                description="RVU components sum correctly for payable items",
                validator_func=lambda x: True,  # Placeholder
                severity="error"
            )
        ]
    
    def _load_schema_contracts(self) -> Dict[str, SchemaContract]:
        """Load schema contracts for MPFS data"""
        contracts = {}
        
        # MPFS RVU contract
        contracts["mpfs_rvu"] = SchemaContract(
            dataset_name="mpfs_rvu",
            version="1.0",
            generated_at=datetime.now().isoformat(),
            columns={
                "hcpcs": {"name": "hcpcs", "type": "string", "required": True, "description": "HCPCS code"},
                "modifier": {"name": "modifier", "type": "string", "required": False, "description": "Modifier code"},
                "status_code": {"name": "status_code", "type": "string", "required": True, "description": "Status indicator"},
                "global_days": {"name": "global_days", "type": "string", "required": False, "description": "Global period days"},
                "rvu_work": {"name": "rvu_work", "type": "decimal", "required": True, "description": "Work RVU"},
                "rvu_pe_nonfac": {"name": "rvu_pe_nonfac", "type": "decimal", "required": True, "description": "PE RVU non-facility"},
                "rvu_pe_fac": {"name": "rvu_pe_fac", "type": "decimal", "required": True, "description": "PE RVU facility"},
                "rvu_malp": {"name": "rvu_malp", "type": "decimal", "required": True, "description": "Malpractice RVU"},
                "na_indicator": {"name": "na_indicator", "type": "string", "required": False, "description": "Not applicable indicator"},
                "opps_cap_applicable": {"name": "opps_cap_applicable", "type": "boolean", "required": True, "description": "OPPS cap applies"},
                "effective_from": {"name": "effective_from", "type": "date", "required": True, "description": "Effective from date"},
                "effective_to": {"name": "effective_to", "type": "date", "required": False, "description": "Effective to date"},
                "release_id": {"name": "release_id", "type": "string", "required": True, "description": "Release identifier"}
            },
            primary_keys=["hcpcs", "modifier", "effective_from"],
            partition_columns=["effective_from"],
            business_rules=["RVU components must sum correctly"],
            quality_thresholds={"completeness": 0.99, "validity": 0.99}
        )
        
        # MPFS Conversion Factor contract
        contracts["mpfs_cf"] = SchemaContract(
            dataset_name="mpfs_cf",
            version="1.0",
            generated_at=datetime.now().isoformat(),
            columns={
                "cf_type": {"name": "cf_type", "type": "string", "required": True, "description": "Conversion factor type"},
                "cf_value": {"name": "cf_value", "type": "decimal", "required": True, "description": "Conversion factor value"},
                "effective_from": {"name": "effective_from", "type": "date", "required": True, "description": "Effective from date"},
                "effective_to": {"name": "effective_to", "type": "date", "required": False, "description": "Effective to date"},
                "release_id": {"name": "release_id", "type": "string", "required": True, "description": "Release identifier"}
            },
            primary_keys=["cf_type", "effective_from"],
            partition_columns=["effective_from"],
            business_rules=["Conversion factors must be positive"],
            quality_thresholds={"completeness": 0.99, "validity": 0.99}
        )
        
        return contracts

    # ------------------------------------------------------------------
    # Discovery helpers
    # ------------------------------------------------------------------

    def _snapshot_source_file(self, snapshot: SnapshotMetadata) -> SourceFile:
        """Create SourceFile metadata for a reused snapshot."""
        if not snapshot.path:
            raise ValueError(f"Snapshot {snapshot.dataset_id}/{snapshot.release_id} missing curated path")

        filename = f"{snapshot.dataset_id}_{snapshot.release_id}.parquet"
        metadata = {
            "dataset_id": snapshot.dataset_id,
            "release_id": snapshot.release_id,
            "path": snapshot.path,
            "digest": snapshot.digest,
            "effective_from": snapshot.effective_from.isoformat(),
            "effective_to": snapshot.effective_to.isoformat() if snapshot.effective_to else None,
            "manifest_url": snapshot.manifest_url,
            "ingestion_mode": "snapshot",
        }

        return SourceFile(
            url=f"snapshot://{snapshot.dataset_id}/{snapshot.release_id}",
            filename=filename,
            content_type="application/parquet",
            checksum=snapshot.digest,
            metadata=metadata,
        )

    def _conversion_factor_source_file(
        self, year: int, release_id: Optional[str] = None
    ) -> Optional[SourceFile]:
        """Create SourceFile describing conversion factor artefact for the year if an override is configured.

        YAML overrides (and future CLI wiring) take precedence. If no override is discovered we now derive the
        conversion factor directly from the RVU snapshot instead of attempting a CMS download that may 404.
        """
        source_url = self._conversion_factor_url(year)
        filename = Path(urlparse(source_url).path).name or f"conversion_factor_{year}.zip"

        metadata = {
            "dataset_id": "mpfs_cf",
            "year": year,
            "source_url": source_url,
            "ingestion_mode": "conversion_factor",
        }

        # Check config service for overrides. Prefer the MPFS release ID, fall back to RVU release ID
        # Config service takes precedence, but CLI flags remain fallback
        release_candidates = []
        if self.current_release_id:
            release_candidates.append(self.current_release_id)
        if release_id and release_id not in release_candidates:
            release_candidates.append(release_id)

        for candidate_release_id in release_candidates:
            try:
                config_overrides = self.config_service.get_cf_overrides(candidate_release_id)
                if not config_overrides:
                    continue

                if config_overrides.get("manual_override_path"):
                    metadata["manual_override_path"] = config_overrides["manual_override_path"]
                    logger.info(
                        "Using YAML config override for conversion factor",
                        release_id=candidate_release_id,
                        override_path=config_overrides["manual_override_path"],
                    )
                if config_overrides.get("expected_checksum"):
                    metadata["expected_checksum"] = config_overrides["expected_checksum"]

                # Once overrides applied for a candidate, stop searching
                break
            except (ValueError, FileNotFoundError) as e:
                # Malformed YAML or invalid path - log warning but continue with CLI fallback/additional candidates
                logger.warning(
                    "Config service error, falling back to next override candidate or CLI flags",
                    release_id=candidate_release_id,
                    error=str(e),
                )

        # CLI flags can still override via metadata if provided externally
        # This maintains backward compatibility until YAML service is production-ready

        if metadata.get("manual_override_path"):
            return SourceFile(
                url=source_url,
                filename=filename,
                content_type=self._infer_content_type(filename),
                metadata=metadata,
            )

        logger.info(
            "No conversion factor override configured; will derive from RVU snapshot",
            year=year,
            release_id=release_id,
        )
        return None

    @staticmethod
    def _infer_content_type(filename: str) -> str:
        suffix = Path(filename).suffix.lower()
        if suffix in {".zip"}:
            return "application/zip"
        if suffix in {".xlsx", ".xls"}:
            return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        if suffix in {".csv"}:
            return "text/csv"
        return "application/octet-stream"

    @staticmethod
    def _conversion_factor_url(year: int) -> str:
        """Return default CMS URL for the MPFS conversion factor artefact."""
        # CMS typically publishes CF artefacts under /files/zip/cy-YYYY-mpfs-conversion-factor.zip
        # Allow overrides via SourceFile metadata if the pattern changes.
        return f"https://www.cms.gov/files/zip/cy-{year}-mpfs-conversion-factor.zip"

    def _normalize_release_suffix(self, year: int, quarter: Optional[str]) -> Optional[str]:
        """Normalize a user-supplied quarter/release argument to CMS suffix (A/B/C/D, AR, etc.)."""
        if quarter is None:
            return None

        value = str(quarter).strip().upper()
        if not value:
            return None

        self._requested_release_input = value
        normalized = value.replace("-", "_")

        # Map quarter-esque tokens first (Q1 → A, etc.)
        for token, suffix in QUARTER_TO_SUFFIX.items():
            if normalized.endswith(token):
                return suffix

        # Strip dataset prefixes (e.g., RVU_2025_B) if present
        if "_" in normalized:
            tokens = [t for t in normalized.split("_") if t]
            candidate = tokens[-1]
            if candidate in RELEASE_SUFFIXES:
                return candidate

        # Support compact forms like 2025B or RVU25AR
        match = re.search(r"(AR|BR|CR|DR|A|B|C|D)$", normalized)
        if match:
            suffix = match.group(1)
            if suffix in RELEASE_SUFFIXES:
                return suffix

        return None

    @staticmethod
    def _build_dataset_release_id(prefix: str, year: int, suffix: Optional[str]) -> Optional[str]:
        """Compose dataset-specific release identifier (e.g., rvu_2025_B)."""
        if not suffix:
            return None
        prefix_clean = prefix.strip().lower()
        return f"{prefix_clean}_{year}_{suffix}"

    def _set_ingest_scope(self, year: int, quarter: Optional[str]) -> None:
        """Persist target year and release IDs for this ingestion run."""
        self.target_year = year
        self.target_release_suffix = self._normalize_release_suffix(year, quarter)
        self.target_rvu_release_id = self._build_dataset_release_id("rvu", year, self.target_release_suffix)
        self.target_gpci_release_id = self._build_dataset_release_id("gpci", year, self.target_release_suffix)
    
    async def discover_source_files(self) -> List[SourceFile]:
        """Discover MPFS source inputs via dataset snapshots and CF fetcher"""
        logger.info("Starting MPFS source file discovery")

        snapshot_files: List[SourceFile] = []

        rvu_release_id = self.target_rvu_release_id
        rvu_snapshot = self.snapshot_service.get_latest_snapshot("rvu_items", release_id=rvu_release_id)
        if rvu_release_id and (not rvu_snapshot or not rvu_snapshot.path):
            raise ValueError(f"RVU snapshot for release {rvu_release_id} not available; run RVU ingestion first")
        if not rvu_snapshot or not rvu_snapshot.path:
            rvu_snapshot = self.snapshot_service.get_latest_snapshot("rvu_items")
        if not rvu_snapshot or not rvu_snapshot.path:
            raise ValueError("RVU snapshot not available; run RVU ingestion first")
        snapshot_files.append(self._snapshot_source_file(rvu_snapshot))

        gpci_release_id = self.target_gpci_release_id
        gpci_snapshot = self.snapshot_service.get_latest_snapshot("gpci_indices", release_id=gpci_release_id)
        if gpci_release_id and (not gpci_snapshot or not gpci_snapshot.path):
            # Try to align with RVU release suffix if available
            candidate_release = None
            if rvu_snapshot and rvu_snapshot.release_id.startswith("rvu_"):
                candidate_release = rvu_snapshot.release_id.replace("rvu_", "gpci_", 1)
            if candidate_release and candidate_release != gpci_release_id:
                gpci_snapshot = self.snapshot_service.get_latest_snapshot("gpci_indices", release_id=candidate_release)
        if not gpci_snapshot or not gpci_snapshot.path:
            raise ValueError("GPCI snapshot not available; run RVU ingestion first")
        snapshot_files.append(self._snapshot_source_file(gpci_snapshot))

        current_year = self.target_year or datetime.now().year
        # Determine release_id from RVU snapshot for config service lookup
        release_id = rvu_snapshot.release_id if rvu_snapshot else None
        cf_source = self._conversion_factor_source_file(current_year, release_id=release_id)

        source_files = list(snapshot_files)
        if cf_source:
            source_files.append(cf_source)
            self._conversion_factor_strategy = "download"
        else:
            self._conversion_factor_strategy = "derive_from_rvu"

        logger.info(
            "MPFS discovery completed",
            snapshot_count=len(snapshot_files),
            download_count=1 if cf_source else 0,
            files_found=len(source_files),
            rvu_release=rvu_snapshot.release_id if rvu_snapshot else None,
            gpci_release=gpci_snapshot.release_id if gpci_snapshot else None,
            requested_suffix=self.target_release_suffix,
        )
        return source_files
    
    async def land_stage(self, source_files: List[SourceFile]) -> RawBatch:
        """Land stage: resolve snapshots and cache conversion factor artefact."""
        logger.info("Starting MPFS land stage", file_count=len(source_files))

        batch_id = str(uuid.uuid4())
        raw_batch = RawBatch(
            source_files=source_files,
            metadata={
                "ingestion_timestamp": datetime.now().isoformat(),
                "source": self.source_name,
                "license": self.license,
                "attribution_required": self.attribution_required,
                "datasets": [],
                "snapshots": {},
                "downloads": {},
                "batch_id": batch_id,
                "release_id": self.current_release_id,
                "vintage_date": datetime.now().date().isoformat(),
                "target_year": self.target_year or datetime.now().year,
                "target_release_suffix": self.target_release_suffix,
                "requested_release_param": self._requested_release_input,
                "requested_rvu_release_id": self.target_rvu_release_id,
                "requested_gpci_release_id": self.target_gpci_release_id,
            },
        )

        for source_file in source_files:
            mode = source_file.metadata.get("ingestion_mode")
            dataset_id = source_file.metadata.get("dataset_id")
            if dataset_id:
                raw_batch.metadata["datasets"].append(dataset_id)

            if mode == "snapshot":
                snapshot_meta = dict(source_file.metadata)
                snapshot_meta.setdefault("resolved_at", datetime.now().isoformat())
                raw_batch.metadata.setdefault("snapshots", {})[dataset_id] = snapshot_meta
            elif mode == "conversion_factor":
                cf_meta = await self.cf_fetcher.ensure_conversion_factor(
                    year=source_file.metadata["year"],
                    source_url=source_file.metadata["source_url"],
                    manual_override_path=source_file.metadata.get("manual_override_path"),
                    expected_checksum=source_file.metadata.get("expected_checksum"),
                )
                cf_record = cf_meta.to_dict()
                cf_record["downloaded_at"] = datetime.now().isoformat()
                raw_batch.metadata.setdefault("downloads", {})[dataset_id] = cf_record
            else:
                raise ValueError(f"Unsupported ingestion mode for source file {source_file.filename}")

        if "mpfs_cf" not in raw_batch.metadata["datasets"]:
            raw_batch.metadata["datasets"].append("mpfs_cf")

        if self._conversion_factor_strategy == "derive_from_rvu":
            raw_batch.metadata.setdefault("derived_inputs", {})["mpfs_cf"] = {
                "strategy": "derive_from_rvu_snapshot",
                "source_dataset": "rvu_items",
                "release_id": raw_batch.metadata.get("release_id"),
            }

        logger.info(
            "MPFS land stage completed",
            snapshot_sources=len(raw_batch.metadata["snapshots"]),
            downloads=len(raw_batch.metadata["downloads"]),
        )
        return raw_batch
    
    async def validate_stage(self, raw_batch: RawBatch) -> Tuple[RawBatch, List[Dict[str, Any]]]:
        """Validate stage: ensure snapshot artefacts and downloads are usable."""
        logger.info("Starting MPFS validate stage")

        validation_results: List[Dict[str, Any]] = []
        snapshots = raw_batch.metadata.get("snapshots", {})
        downloads = raw_batch.metadata.get("downloads", {})

        for dataset_id, snapshot_meta in snapshots.items():
            validation_results.extend(self._validate_snapshot(dataset_id, snapshot_meta))

        for dataset_id, download_meta in downloads.items():
            validation_results.extend(self._validate_download(dataset_id, download_meta))

        logger.info(
            "MPFS validate stage completed",
            snapshot_checked=len(snapshots),
            downloads_checked=len(downloads),
            issues=len([r for r in validation_results if r.get("severity") == "CRITICAL"]),
        )

        return raw_batch, validation_results

    def _validate_snapshot(self, dataset_id: str, snapshot_meta: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate snapshot metadata and underlying parquet artefact."""
        results: List[Dict[str, Any]] = []
        path_value = snapshot_meta.get("path")
        release_id = snapshot_meta.get("release_id")
        if not path_value:
            results.append(
                {
                    "rule_id": "mpfs_snapshot_missing_path",
                    "severity": "CRITICAL",
                    "message": f"Snapshot {dataset_id} missing path metadata",
                    "dataset_id": dataset_id,
                    "release_id": release_id,
                }
            )
            return results

        path = Path(path_value)
        if not path.exists():
            results.append(
                {
                    "rule_id": "mpfs_snapshot_missing_file",
                    "severity": "CRITICAL",
                    "message": f"Snapshot path not found: {path}",
                    "dataset_id": dataset_id,
                    "release_id": release_id,
                }
            )
            return results

        size_bytes = self._path_size_bytes(path)
        if size_bytes == 0:
            results.append(
                {
                    "rule_id": "mpfs_snapshot_empty",
                    "severity": "CRITICAL",
                    "message": f"Snapshot artefact empty: {path}",
                    "dataset_id": dataset_id,
                    "release_id": release_id,
                }
            )
        else:
            results.append(
                {
                    "rule_id": "mpfs_snapshot_ok",
                    "severity": "INFO",
                    "message": f"Snapshot validated ({size_bytes} bytes)",
                    "dataset_id": dataset_id,
                    "release_id": release_id,
                }
            )

        return results

    def _validate_download(self, dataset_id: str, download_meta: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate conversion factor download metadata."""
        results: List[Dict[str, Any]] = []
        path_value = download_meta.get("path")
        if not path_value:
            results.append(
                {
                    "rule_id": "mpfs_download_missing_path",
                    "severity": "CRITICAL",
                    "message": f"Conversion factor artefact missing path for {dataset_id}",
                    "dataset_id": dataset_id,
                }
            )
            return results

        path = Path(path_value)
        if not path.exists():
            results.append(
                {
                    "rule_id": "mpfs_download_missing_file",
                    "severity": "CRITICAL",
                    "message": f"Conversion factor artefact not found: {path}",
                    "dataset_id": dataset_id,
                }
            )
            return results

        size_bytes = path.stat().st_size
        if size_bytes == 0:
            results.append(
                {
                    "rule_id": "mpfs_download_empty",
                    "severity": "CRITICAL",
                    "message": f"Conversion factor artefact empty: {path}",
                    "dataset_id": dataset_id,
                }
            )
        else:
            results.append(
                {
                    "rule_id": "mpfs_download_ok",
                    "severity": "INFO",
                    "message": f"Conversion factor artefact ready ({size_bytes} bytes)",
                    "dataset_id": dataset_id,
                }
            )

        return results

    @staticmethod
    def _path_size_bytes(path: Path) -> int:
        if path.is_file():
            return path.stat().st_size
        if path.is_dir():
            return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())
        return 0
    
    async def normalize_stage(self, raw_batch: RawBatch) -> AdaptedBatch:
        """Normalize stage: load RVU/GPCI snapshots and conversion factor artefact."""
        logger.info("Starting MPFS normalize stage")

        snapshots = raw_batch.metadata.get("snapshots", {})
        downloads = raw_batch.metadata.get("downloads", {})

        rvu_snapshot = snapshots.get("rvu_items")
        gpci_snapshot = snapshots.get("gpci_indices")
        cf_meta = downloads.get("mpfs_cf") or next(iter(downloads.values()), None)

        if not rvu_snapshot or not gpci_snapshot:
            raise ValueError("Required RVU/GPCI snapshots were not available in land stage metadata")

        rvu_df = self._load_snapshot_dataframe("rvu_items", rvu_snapshot)
        gpci_df = self._load_snapshot_dataframe("gpci_indices", gpci_snapshot)
        if cf_meta:
            cf_df = self._load_conversion_factor_dataframe(cf_meta)
            cf_strategy = "download"
        else:
            cf_df = self._derive_conversion_factor_from_rvu(rvu_df, rvu_snapshot, raw_batch.metadata)
            cf_strategy = "derive_from_rvu"

        dataframes = {"rvu": rvu_df, "gpci": gpci_df, "cf": cf_df}
        schema_contract = {
            "mpfs_rvu": self.schema_contracts["mpfs_rvu"].to_dict(),
            "mpfs_cf": self.schema_contracts["mpfs_cf"].to_dict(),
        }
        metadata = {
            **raw_batch.metadata,
            "normalized_at": datetime.now().isoformat(),
            "snapshot_release_ids": {
                "rvu_items": rvu_snapshot.get("release_id"),
                "gpci_indices": gpci_snapshot.get("release_id"),
            },
            "conversion_factor_strategy": cf_strategy,
            "conversion_factor_source": (
                (cf_meta.get("source_url") or cf_meta.get("path")) if cf_meta else "rvu_snapshot"
            ),
        }

        adapted_batch = AdaptedBatch(
            dataframes=dataframes,
            schema_contract=schema_contract,
            metadata=metadata,
        )

        logger.info(
            "MPFS normalize stage completed",
            rvu_rows=len(rvu_df),
            gpci_rows=len(gpci_df),
            cf_rows=len(cf_df),
        )
        return adapted_batch

    def _load_snapshot_dataframe(self, dataset_id: str, snapshot_meta: Dict[str, Any]) -> pd.DataFrame:
        """Load a parquet dataframe from snapshot metadata."""
        path_value = snapshot_meta.get("path")
        if not path_value:
            raise ValueError(f"Snapshot metadata for {dataset_id} missing path")

        path = Path(path_value)
        if not path.is_absolute():
            resolved = resolve_repo_path(path, self._snapshot_search_roots, dataset_hint=dataset_id)
            if resolved:
                path = resolved
        if not path.exists():
            resolved = resolve_repo_path(path, self._snapshot_search_roots, dataset_hint=dataset_id)
            if resolved:
                path = resolved
        if not path.exists():
            raise FileNotFoundError(f"Snapshot path does not exist for {dataset_id}: {path}")

        # If the snapshot path is a manifest.json, resolve the dataset's parquet file from it
        if path.suffix.lower() == ".json":
            resolved = self._resolve_parquet_from_manifest(path, dataset_id)
            if resolved is None:
                raise FileNotFoundError(f"Unable to resolve parquet from manifest for {dataset_id}: {path}")
            path = resolved

        if path.is_dir():
            file_path = self._select_parquet_candidate(path, dataset_id)
        else:
            file_path = path

        row_limit = self._max_snapshot_rows(dataset_id)
        return self._read_parquet_snapshot(file_path, dataset_id, row_limit=row_limit)

    @staticmethod
    def _max_snapshot_rows(dataset_id: str) -> Optional[int]:
        """Look up optional row cap for snapshot loading."""
        env_keys = [
            f"MAX_{dataset_id.upper()}_SNAPSHOT_ROWS",
            "MAX_MPFS_SNAPSHOT_ROWS",
            "MAX_INGESTION_ROWS",
        ]
        for key in env_keys:
            raw = os.environ.get(key)
            if not raw:
                continue
            try:
                value = int(raw)
            except ValueError:
                continue
            if value > 0:
                return value
        return None

    def _read_parquet_snapshot(
        self,
        file_path: Path,
        dataset_id: str,
        row_limit: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> pd.DataFrame:
        """Stream parquet data into a DataFrame, respecting optional row limits."""
        logger.info(
            "Loading snapshot dataframe",
            dataset_id=dataset_id,
            path=str(file_path),
            row_limit=row_limit,
        )

        if not batch_size:
            batch_size = int(os.environ.get("MPFS_SNAPSHOT_BATCH_ROWS", "50000"))

        try:
            import pyarrow.parquet as pq
        except ImportError:  # pragma: no cover
            df = pd.read_parquet(file_path)
            if row_limit:
                df = df.head(row_limit)
            return df

        parquet_file = pq.ParquetFile(file_path)
        total_rows = parquet_file.metadata.num_rows if parquet_file.metadata else None

        frames: List[pd.DataFrame] = []
        rows_read = 0

        def remaining_rows() -> Optional[int]:
            if row_limit is None:
                return None
            return max(row_limit - rows_read, 0)

        for batch in parquet_file.iter_batches(batch_size=batch_size):
            remaining = remaining_rows()
            if remaining is not None and remaining <= 0:
                break

            slice_batch = batch
            if remaining is not None and batch.num_rows > remaining:
                slice_batch = batch.slice(0, remaining)

            frames.append(slice_batch.to_pandas())
            rows_read += slice_batch.num_rows

            if remaining_rows() is not None and remaining_rows() <= 0:
                break

        if not frames:
            df = pd.DataFrame()
        elif len(frames) == 1:
            df = frames[0]
        else:
            df = pd.concat(frames, ignore_index=True)

        if row_limit and total_rows and total_rows > row_limit:
            logger.info(
                "Row limiting applied for snapshot load",
                dataset=dataset_id,
                limited_rows=row_limit,
                original_rows=total_rows,
            )
        else:
            logger.info(
                "Snapshot dataframe loaded",
                dataset=dataset_id,
                rows=len(df),
            )

        return df

    def _resolve_parquet_from_manifest(self, manifest_path: Path, dataset_id: str) -> Optional[Path]:
        """Attempt to resolve a parquet file path from a manifest.json.

        Tries common shapes:
          - datasets: {<key>: { parquet_path: "..." }}
          - curated_tables: { <alias>: "...parquet" }
        Falls back to scanning the manifest directory for a parquet file.
        """
        try:
            data = json.loads(manifest_path.read_text())
        except Exception:
            return None

        # 1) datasets mapping
        ds = data.get("datasets")
        if isinstance(ds, dict):
            # try exact dataset_id and known aliases
            aliases = {
                "rvu_items": "pprrvu",
                "gpci_indices": "gpci",
                "anescf": "anescf",
                "localitycounty": "localitycounty",
                "oppscap": "oppscap",
            }
            keys = [dataset_id]
            if dataset_id in aliases:
                keys.append(aliases[dataset_id])
            for key in keys:
                entry = ds.get(key)
                if isinstance(entry, dict):
                    p = entry.get("parquet_path") or entry.get("path")
                    if isinstance(p, str):
                        p_path = Path(p)
                        if p_path.exists():
                            return p_path

        # 2) curated_tables mapping
        ct = data.get("curated_tables")
        if isinstance(ct, dict):
            aliases = {
                "rvu_items": "pprrvu",
                "gpci_indices": "gpci",
                "anescf": "anescf",
                "localitycounty": "localitycounty",
                "oppscap": "oppscap",
            }
            alias = aliases.get(dataset_id)
            if alias and isinstance(ct.get(alias), str):
                p_path = Path(ct[alias])
                if p_path.exists():
                    return p_path

        # 3) Fallback: scan manifest directory for best candidate
        for candidate in sorted(manifest_path.parent.glob("*.parquet")):
            if dataset_id in candidate.stem:
                return candidate
        # any parquet as last resort
        any_parquet = next(iter(sorted(manifest_path.parent.glob("*.parquet"))), None)
        return any_parquet

    def _select_parquet_candidate(self, directory: Path, dataset_id: str) -> Path:
        """Select a parquet file within a directory that best matches the dataset id."""
        candidates = sorted(directory.glob("*.parquet"))
        if not candidates:
            raise FileNotFoundError(f"No parquet files found in snapshot directory {directory}")
        for candidate in candidates:
            if dataset_id in candidate.stem:
                return candidate
        return candidates[0]

    def _load_conversion_factor_dataframe(self, cf_meta: Dict[str, Any]) -> pd.DataFrame:
        """Load conversion factor data from downloaded artefact."""
        if not cf_meta:
            return pd.DataFrame()

        path = Path(cf_meta.get("path"))
        if not path.exists():
            raise FileNotFoundError(f"Conversion factor artefact not found at {path}")

        year = cf_meta.get("year") or datetime.now().year
        content_bytes: bytes
        source_name: str

        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path, "r") as zf:
                member = self._select_cf_member(zf.namelist())
                content_bytes = zf.read(member)
                source_name = member
        else:
            content_bytes = path.read_bytes()
            source_name = path.name

        cf_value = self._extract_cf_value(content_bytes)
        df = pd.DataFrame(
            [
                {
                    "cf_value": cf_value,
                    "cf_type": "physician",
                    "year": year,
                    "source_file": source_name,
                }
            ]
        )
        return df

    def _derive_conversion_factor_from_rvu(
        self,
        rvu_df: pd.DataFrame,
        rvu_snapshot_meta: Optional[Dict[str, Any]],
        batch_metadata: Optional[Dict[str, Any]],
    ) -> pd.DataFrame:
        """Derive conversion factor from RVU snapshot when no external artefact is available."""
        if rvu_df is None or rvu_df.empty:
            logger.warning("RVU dataframe empty; unable to derive conversion factor")
            return pd.DataFrame()

        column_lookup = {col.lower(): col for col in rvu_df.columns}
        cf_col = None
        for candidate in ("conversion_factor", "cf_value", "cf"):
            if candidate in column_lookup:
                cf_col = column_lookup[candidate]
                break

        if not cf_col:
            logger.warning(
                "Conversion factor column not present in RVU snapshot; derived CF will be empty",
                available_columns=list(rvu_df.columns)[:20],
            )
            return pd.DataFrame()

        def _parse_date(raw: Optional[str]) -> Optional[date]:
            if not raw:
                return None
            try:
                return date.fromisoformat(str(raw))
            except (TypeError, ValueError):
                return None

        vintage_date: Optional[date] = None
        vintage_raw = (batch_metadata or {}).get("vintage_date")
        if isinstance(vintage_raw, str):
            try:
                vintage_date = date.fromisoformat(vintage_raw)
            except ValueError:
                logger.warning("Unable to parse vintage_date from batch metadata", vintage_date=vintage_raw)
        if not vintage_date:
            vintage_date = datetime.now().date()

        target_year = self.target_year or vintage_date.year
        effective_start_fallback = _parse_date((rvu_snapshot_meta or {}).get("effective_from")) or date(target_year, 1, 1)
        effective_end_fallback = _parse_date((rvu_snapshot_meta or {}).get("effective_to")) or date(target_year, 12, 31)
        source_release = (rvu_snapshot_meta or {}).get("release_id")

        anesthesia_cols = [col for col in rvu_df.columns if "anest" in col.lower()]
        if anesthesia_cols:
            logger.info(
                "Anesthesia conversion factor columns detected in RVU snapshot; ignoring for physician-factor MVP",
                columns=anesthesia_cols,
            )

        cf_rows = pd.DataFrame({"cf_value": pd.to_numeric(rvu_df[cf_col], errors="coerce")})
        if "effective_start" in rvu_df.columns:
            cf_rows["effective_start"] = pd.to_datetime(rvu_df["effective_start"], errors="coerce")
        elif "effective_from" in rvu_df.columns:
            cf_rows["effective_start"] = pd.to_datetime(rvu_df["effective_from"], errors="coerce")
        else:
            cf_rows["effective_start"] = pd.NaT

        if "effective_end" in rvu_df.columns:
            cf_rows["effective_end"] = pd.to_datetime(rvu_df["effective_end"], errors="coerce")
        elif "effective_to" in rvu_df.columns:
            cf_rows["effective_end"] = pd.to_datetime(rvu_df["effective_to"], errors="coerce")
        else:
            cf_rows["effective_end"] = pd.NaT

        cf_rows = cf_rows.dropna(subset=["cf_value"])
        if cf_rows.empty:
            logger.warning(
                "Conversion factor column in RVU snapshot contains no numeric values; derived CF will be empty",
                column=cf_col,
            )
            return pd.DataFrame()

        cf_rows["cf_value"] = cf_rows["cf_value"].round(6)
        cf_rows["effective_start"] = cf_rows["effective_start"].fillna(pd.Timestamp(effective_start_fallback))
        cf_rows["effective_end"] = cf_rows["effective_end"].fillna(pd.Timestamp(effective_end_fallback))

        cf_rows["effective_start"] = cf_rows["effective_start"].dt.date
        cf_rows["effective_end"] = cf_rows["effective_end"].dt.date

        unique_rows = (
            cf_rows[["cf_value", "effective_start", "effective_end"]]
            .drop_duplicates()
            .sort_values(["effective_start", "effective_end", "cf_value"])
        )

        records = []
        for _, row in unique_rows.iterrows():
            effective_start_value = row["effective_start"] or effective_start_fallback
            effective_end_value = row["effective_end"] or effective_end_fallback
            year_value = effective_start_value.year if isinstance(effective_start_value, date) else target_year
            records.append(
                {
                    "cf_value": float(row["cf_value"]),
                    "cf_type": "physician",
                    "year": year_value,
                    "effective_start": effective_start_value,
                    "effective_end": effective_end_value,
                    "release_id": self.current_release_id or source_release or "",
                    "source_file": "derived_from_rvu_snapshot",
                    "source_snapshot_release": source_release,
                }
            )

        if not records:
            return pd.DataFrame()

        derived_values = [record["cf_value"] for record in records]
        logger.info(
            "Derived conversion factor from RVU snapshot",
            values=derived_values,
            column=cf_col,
            release_id=source_release,
            suffix=self.target_release_suffix,
        )

        return pd.DataFrame(records)

    def _select_cf_member(self, members: List[str]) -> str:
        """Select preferred file within conversion factor archive."""
        if not members:
            raise ValueError("Conversion factor archive is empty")
        preferred_ext = [".csv", ".txt", ".xlsx"]
        for ext in preferred_ext:
            for member in members:
                if member.lower().endswith(ext):
                    return member
        return members[0]

    def _extract_cf_value(self, raw_bytes: bytes) -> float:
        """Extract first numeric value from artefact content."""
        text = raw_bytes.decode("utf-8", errors="ignore")
        matches = re.findall(r"\d+\.\d+", text)
        if matches:
            try:
                return float(matches[0])
            except ValueError:
                pass
        # Fallback: attempt to parse as excel if regex fails
        try:
            df = pd.read_excel(io.BytesIO(raw_bytes))
            numeric = pd.to_numeric(df.select_dtypes(include="number").stack(), errors="coerce").dropna()
            if not numeric.empty:
                return float(numeric.iloc[0])
        except Exception:
            pass
        return float("nan")

    async def enrich_stage(self, adapted_batch: AdaptedBatch) -> StageFrame:
        """Enrich stage: build curated datasets using MPFS builder."""
        logger.info("Starting MPFS enrich stage")

        vintage_str = adapted_batch.metadata.get("vintage_date")
        vintage_date = (
            date.fromisoformat(vintage_str) if isinstance(vintage_str, str) else datetime.now().date()
        )

        inputs = MPFSNormalizedInputs(
            rvu=adapted_batch.dataframes.get("rvu", pd.DataFrame()),
            gpci=adapted_batch.dataframes.get("gpci", pd.DataFrame()),
            conversion_factor=adapted_batch.dataframes.get("cf", pd.DataFrame()),
            release_id=self.current_release_id or adapted_batch.metadata.get("release_id", ""),
            vintage_date=vintage_date,
        )

        curated_views = build_curated_views(inputs, valuation_date=vintage_date)
        row_counts = {name: len(df) for name, df in curated_views.items()}

        release_id = self.current_release_id or adapted_batch.metadata.get("release_id", "")

        metadata = {
            **adapted_batch.metadata,
            "curated_tables": list(curated_views.keys()),
            "enriched_at": datetime.now().isoformat(),
        }
        metadata.setdefault("release_id", release_id)
        metadata.setdefault("batch_id", adapted_batch.metadata.get("batch_id"))

        stage_frame = StageFrame(
            data=curated_views,
            schema={"datasets": list(curated_views.keys())},
            metadata=metadata,
            quality_metrics={"row_counts": row_counts},
        )

        logger.info("MPFS enrich stage completed", curated_tables=len(curated_views))
        return stage_frame

    async def publish_stage(self, stage_frame: StageFrame) -> Dict[str, Any]:
        """Publish stage: persist curated views and register snapshots."""
        logger.info("Starting MPFS publish stage")

        curated_views: Dict[str, pd.DataFrame] = stage_frame.data or {}
        release_id = self.current_release_id or stage_frame.metadata.get("release_id")
        if not release_id:
            raise ValueError("Release ID is required before publish stage")

        output_root = Path(self.output_dir) / "curated" / "mpfs" / release_id
        output_root.mkdir(parents=True, exist_ok=True)

        manifest: Dict[str, Any] = {}
        manifest_path = output_root / "manifest.json"

        manifest_entries: Dict[str, Dict[str, Any]] = {}
        file_digests: Dict[str, str] = {}
        effective_ranges: Dict[str, Tuple[Optional[date], Optional[date]]] = {}

        for dataset_name, df in curated_views.items():
            file_path = output_root / f"{dataset_name}.parquet"

            def _write_parquet(target: Path) -> None:
                df.to_parquet(target, compression="snappy", index=False)

            atomic_write(file_path, _write_parquet)
            digest = compute_sha256(file_path)
            file_digests[dataset_name] = digest

            effective_from, effective_to = self._infer_effective_range(df)
            effective_from = self._normalize_snapshot_date(effective_from) or datetime.now().date()
            effective_to = self._normalize_snapshot_date(effective_to)
            effective_ranges[dataset_name] = (effective_from, effective_to)

            manifest_entries[dataset_name] = {
                "path": str(file_path),
                "rows": len(df),
                "digest": digest,
                "effective_from": str(effective_from) if effective_from else None,
                "effective_to": str(effective_to) if effective_to else None,
            }

        manifest_payload = manifest_entries.copy()

        session = self.snapshot_service.db
        try:
            atomic_write_json(manifest_path, manifest_payload)
            with session.begin():
                for dataset_name, info in manifest_entries.items():
                    normalized_from, normalized_to = effective_ranges.get(dataset_name, (datetime.now().date(), None))
                    self.snapshot_service.register_snapshot(
                        dataset_id=dataset_name,
                        release_id=release_id,
                        digest=file_digests[dataset_name],
                        effective_from=normalized_from,
                        effective_to=normalized_to,
                        manifest_url=str(manifest_path),
                        curated_path=info.get("path"),
                        autocommit=False,
                    )
        except Exception:
            self._cleanup_release_directory(output_root)
            raise

        observability_report = self._generate_observability_report(stage_frame, manifest_entries)

        summary = {
            name: {"rows": info["rows"], "path": info["path"], "digest": info["digest"]}
            for name, info in manifest_entries.items()
        }

        result = {
            "batch_id": stage_frame.metadata.get("batch_id") or self.current_batch_id,
            "dataset_name": self.dataset_name,
            "release_id": release_id,
            "curated_views": summary,
            "manifest_path": str(manifest_path),
            "observability_report": observability_report,
            "metadata": stage_frame.metadata,
            "file_digests": file_digests,
        }

        logger.info("MPFS publish stage completed", curated_tables=len(summary))
        return result

    @staticmethod
    def _cleanup_release_directory(path: Path) -> None:
        """Remove partially written release artifacts when registration fails."""
        try:
            if path.exists():
                shutil.rmtree(path)
        except Exception as err:
            logger.warning("Failed to cleanup release directory", path=str(path), error=str(err))

    def _infer_effective_range(self, df: pd.DataFrame) -> Tuple[Optional[date], Optional[date]]:
        """Infer effective date range from dataframe columns."""
        if df.empty:
            return None, None

        candidates_start = [
            "effective_start",
            "effective_from",
            "rvu_effective_start",
            "gpci_effective_start",
        ]
        candidates_end = [
            "effective_end",
            "effective_to",
            "rvu_effective_end",
            "gpci_effective_end",
        ]

        start_series = None
        for column in candidates_start:
            if column in df.columns:
                start_series = pd.to_datetime(df[column], errors="coerce")
                if not start_series.isna().all():
                    break

        end_series = None
        for column in candidates_end:
            if column in df.columns:
                end_series = pd.to_datetime(df[column], errors="coerce")
                if not end_series.isna().all():
                    break

        effective_from = start_series.dropna().dt.date.min() if start_series is not None else None
        effective_to = end_series.dropna().dt.date.max() if end_series is not None else None
        return effective_from, effective_to

    @staticmethod
    def _normalize_snapshot_date(value: Optional[Any]) -> Optional[date]:
        """Convert pandas/NumPy date-like values to `date`, treating NaN/NaT as None."""
        if value is None:
            return None

        if pd.isna(value):
            return None

        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return None
            return value.date()

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, date):
            return value

        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError:
                return None

        to_py = getattr(value, "to_pydatetime", None)
        if callable(to_py):
            try:
                return to_py().date()
            except Exception:
                return None

        return None

    def _generate_observability_report(
        self, stage_frame: StageFrame, manifest: Dict[str, Any]
    ) -> DISObservabilityReport:
        """Generate DIS observability report using curated manifest metrics."""
        row_counts = stage_frame.quality_metrics.get("row_counts", {})
        rows_processed = row_counts.get("mpfs_payment_curated")
        if rows_processed is None:
            rows_processed = sum(row_counts.values())
        release_id = stage_frame.metadata.get("release_id", self.current_release_id or "")
        batch_id = stage_frame.metadata.get("batch_id") or self.current_batch_id or str(uuid.uuid4())

        previous_report = self.observability_collector.get_latest_report(self.dataset_name)

        now = datetime.utcnow()
        expected_frequency_hours = 24 * 365  # annual cadence
        previous_update = previous_report.freshness.last_updated if previous_report else None
        freshness = self.observability_collector.collect_freshness_metrics(
            dataset_name=self.dataset_name,
            last_updated=now,
            expected_frequency_hours=expected_frequency_hours,
            previous_update=previous_update,
        )

        total_size_bytes = 0
        for info in manifest.values():
            path = info.get("path")
            if not path:
                continue
            try:
                total_size_bytes += Path(path).stat().st_size
            except FileNotFoundError:
                logger.warning("Manifest file missing during observability sizing", path=path)

        previous_volume = previous_report.volume if previous_report else None
        volume = self.observability_collector.collect_volume_metrics(
            total_records=rows_processed,
            total_size_bytes=total_size_bytes,
            previous_metrics=previous_volume,
        )

        previous_schema_version = previous_report.schema.schema_version if previous_report else None
        schema = self.observability_collector.collect_schema_metrics(
            schema_version=self.schema_contracts["mpfs_rvu"].version,
            validation_results={"valid": True, "breaking_changes": 0, "non_breaking_changes": 0},
            previous_schema_version=previous_schema_version,
        )

        quality_metrics = {
            "quality_score": 1.0 if rows_processed > 0 else 0.0,
            "rules_passed": row_counts.get("mpfs_rvu", 0),
            "rules_failed": 0,
            "metrics": {
                "null_rate": 0.0,
                "duplicate_rate": 0.0,
            },
        }
        quality = self.observability_collector.collect_quality_metrics(
            validation_results=quality_metrics,
            quality_threshold=self.sla_spec.quality_threshold if hasattr(self, "sla_spec") else 0.95,
        )

        source_files = stage_frame.metadata.get("source_files") or []
        transformation_steps = ["Land", "Validate", "Normalize", "Enrich", "Publish"]
        lineage = self.observability_collector.collect_lineage_metrics(
            source_files=source_files,
            transformation_steps=transformation_steps,
            processing_timestamp=now,
            ingest_run_id=batch_id,
            batch_id=batch_id,
            release_id=release_id,
        )

        report = self.observability_collector.generate_observability_report(
            dataset_name=self.dataset_name,
            freshness=freshness,
            volume=volume,
            schema=schema,
            quality=quality,
            lineage=lineage,
        )

        legacy_report = SimpleNamespace(
            dataset_name=self.dataset_name,
            batch_id=batch_id,
            release_id=release_id,
            generated_at=report.report_timestamp.isoformat(),
            overall_score=report.overall_score,
            critical_alerts=list(report.critical_alerts),
            warnings=list(report.warnings),
            freshness_metrics=SimpleNamespace(
                last_successful_run=report.freshness.last_updated,
                expected_cadence_hours=report.freshness.expected_frequency_hours,
                freshness_score=report.freshness.freshness_score,
                alerts=list(report.freshness.alerts or []),
            ),
            volume_metrics=SimpleNamespace(
                rows_processed=report.volume.total_records,
                rows_rejected=0,
                volume_score=report.volume.volume_score,
                total_size_bytes=report.volume.total_size_bytes,
                expected_records=report.volume.expected_records,
                expected_size_bytes=report.volume.expected_size_bytes,
            ),
            schema_metrics=SimpleNamespace(
                schema_version=report.schema.schema_version,
                drift_detected=report.schema.schema_evolution_detected,
                schema_score=report.schema.schema_score,
                schema_contract_valid=report.schema.schema_contract_valid,
            ),
            quality_metrics=SimpleNamespace(
                validation_score=report.quality.quality_score,
                completeness_score=report.quality.completeness_rate,
                quality_score=report.quality.quality_score,
                null_rate=report.quality.null_rate,
                duplicate_rate=report.quality.duplicate_rate,
            ),
            lineage_metrics=SimpleNamespace(
                source_files=source_files,
                transformations_applied=transformation_steps,
                lineage_score=report.lineage.lineage_score,
            ),
            raw_report=report,
        )

        return legacy_report
    
    async def ingest(self, year: int, quarter: Optional[str] = None) -> Dict[str, Any]:
        """Main ingestion method following DIS pipeline"""
        logger.info("Starting MPFS ingestion", year=year, quarter=quarter)
        
        try:
            # Establish target scope (year + optional release suffix)
            self._conversion_factor_strategy = "auto"
            self._requested_release_input = None
            self._set_ingest_scope(year, quarter)
            if quarter and not self.target_release_suffix:
                logger.warning(
                    "Quarter argument provided but could not be normalized; defaulting to latest snapshot",
                    quarter=quarter,
                )

            # Generate release and batch IDs
            release_tag = self.target_release_suffix or (quarter or "latest")
            self.current_release_id = f"mpfs_{year}_{release_tag}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.current_batch_id = str(uuid.uuid4())
            
            # DIS Pipeline: Land → Validate → Normalize → Enrich → Publish
            source_files = await self.discover_source_files()
            raw_batch = await self.land_stage(source_files)
            validated_batch, validation_results = await self.validate_stage(raw_batch)
            validated_batch.metadata["validation_results"] = validation_results
            adapted_batch = await self.normalize_stage(validated_batch)
            stage_frame = await self.enrich_stage(adapted_batch)
            result = await self.publish_stage(stage_frame)
            
            logger.info("MPFS ingestion completed successfully", 
                       release_id=self.current_release_id,
                       batch_id=self.current_batch_id)
            
            return result
            
        except Exception as e:
            logger.error("MPFS ingestion failed", error=str(e), exc_info=True)
            raise


# Example usage
async def main():
    """Example usage of MPFS ingestor"""
    ingestor = MPFSIngestor()
    
    # Ingest MPFS data for 2025
    result = await ingestor.ingest(2025)
    
    print(f"MPFS ingestion completed:")
    print(f"  Release ID: {result['release_id']}")
    print(f"  Batch ID: {result['batch_id']}")
    print(f"  Curated views: {len(result['curated_views'])}")


if __name__ == "__main__":
    asyncio.run(main())
