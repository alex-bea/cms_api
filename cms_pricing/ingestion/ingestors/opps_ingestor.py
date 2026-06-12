#!/usr/bin/env python3
"""
CMS OPPS Ingester
=================

DIS-compliant ingester for CMS Hospital Outpatient Prospective Payment System (OPPS) 
quarterly releases. Handles Addendum A/B files, APC payment rates, HCPCS→APC mapping,
and wage index enrichment.

Author: CMS Pricing Platform Team
Version: 1.0.0
DIS Compliance: v1.0
QTS Compliance: v1.0
"""

import asyncio
import hashlib
import os
import json
import logging
import re
import zipfile
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

import pandas as pd
import structlog
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from ..contracts.ingestor_spec import BaseDISIngestor
from ..contracts.schema_registry import SchemaRegistry
from ..contracts.ingestor_spec import (
    IngestorSpec,
    ValidationRule,
    SlaSpec,
    OutputSpec,
    DataClass,
    ValidationSeverity,
)
from ..validators.validation_engine import ValidationEngine
from ..enrichers.data_enrichers import GeographyEnricher
from ..publishers.data_publishers import ParquetPublisher
from ..quarantine.dis_quarantine import QuarantineManager
from ..observability.dis_observability import DISObservabilityCollector
from ..scrapers.cms_opps_scraper import CMSOPPSScraper, ScrapedFileInfo
from ..services.ingestor_artifact_profile import IngestorArtifactProfileService
from ...models.opps import OPPSAPCPayment, OPPSHCPCSCrosswalk, RefSILookup
from ...services.dataset_snapshot_service import DatasetSnapshotService

logger = structlog.get_logger()

TABLE_OPPS_APC_PAYMENT = "opps_apc_payment"
TABLE_OPPS_HCPCS_CROSSWALK = "opps_hcpcs_crosswalk"
TABLE_OPPS_RATES_ENRICHED = "opps_rates_enriched"


class OPPSFileType(Enum):
    """OPPS file types."""

    ADDENDUM_A = "addendum_a"
    ADDENDUM_B = "addendum_b"
    ADDENDUM_ZIP = "addendum_zip"
    I_OCE_NOTES = "i_oce_notes"
    HCPCS_UPDATE = "hcpcs_update"


@dataclass
class OPPSBatchInfo:
    """OPPS batch information."""

    batch_id: str
    year: int
    quarter: int
    release_number: int
    effective_from: date
    effective_to: Optional[date]
    files: List[ScrapedFileInfo]
    discovered_at: datetime
    downloaded_at: Optional[datetime] = None


class OPPSIngestor(BaseDISIngestor):
    """
    DIS-compliant OPPS ingester following the 5-stage pipeline:
    Land → Validate → Normalize → Enrich → Publish
    """

    def __init__(
        self,
        output_dir: Path = None,
        database_url: str = None,
        cpt_masking_enabled: bool = True,
    ):
        super().__init__(output_dir, database_url)

        # OPPS-specific configuration
        self.cpt_masking_enabled = cpt_masking_enabled
        env_sample_dir = os.getenv("OPPS_LOCAL_SAMPLE_DIR")
        scraper_sample_dir = (
            Path(env_sample_dir).expanduser() if env_sample_dir else None
        )
        self.scraper = CMSOPPSScraper(
            output_dir=self.output_dir, local_sample_dir=scraper_sample_dir
        )
        self.local_sample_dir = self.scraper.local_sample_dir
        self.artifact_profile_service = IngestorArtifactProfileService()

        # DIS compliance components
        self.schema_registry = SchemaRegistry()
        self.validation_engine = ValidationEngine()
        self.data_enricher = GeographyEnricher({})  # Empty reference data for now
        self.data_publisher = ParquetPublisher(str(self.output_dir))
        self.quarantine_manager = QuarantineManager()
        self.observability = DISObservabilityCollector()

        # OPPS-specific paths
        self.raw_dir = Path(self.output_dir) / "raw" / "opps"
        self.stage_dir = Path(self.output_dir) / "stage" / "opps"
        self.curated_dir = Path(self.output_dir) / "curated" / "opps"
        self.quarantine_dir = Path(self.output_dir) / "quarantine" / "opps"

        # Create directories
        for dir_path in [
            self.raw_dir,
            self.stage_dir,
            self.curated_dir,
            self.quarantine_dir,
        ]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # Load schema contracts
        self._load_schema_contracts()

        # Initialize validation rules
        self._setup_validation_rules()

        # Initialize SLA specifications
        self._setup_sla_specs()

        # Initialize output specifications
        self._setup_output_specs()

    @property
    def dataset_name(self) -> str:
        """Dataset name for DIS compliance."""
        return "cms_opps"

    @property
    def release_cadence(self) -> str:
        """Release cadence for DIS compliance."""
        return "quarterly"

    @property
    def data_classification(self) -> DataClass:
        """Data classification for DIS compliance."""
        return DataClass.PUBLIC

    @property
    def contract_schema_ref(self) -> str:
        """Schema contract reference for DIS compliance."""
        return "cms_opps:1.0.0"

    @property
    def validators(self) -> List[ValidationRule]:
        """Validation rules for DIS compliance."""
        return self._validation_rules

    @property
    def slas(self) -> SlaSpec:
        """SLA specifications for DIS compliance."""
        return self._sla_spec

    @property
    def outputs(self) -> OutputSpec:
        """Output specifications for DIS compliance."""
        return self._output_spec

    @property
    def classification(self) -> DataClass:
        """Data classification for DIS compliance."""
        return self.data_classification

    @property
    def adapter(self) -> callable:
        """Data adapter for DIS compliance."""
        return self._adapt_raw_data

    @property
    def enricher(self) -> callable:
        """Data enricher for DIS compliance."""
        return self._enrich_data

    def _load_schema_contracts(self):
        """Load OPPS schema contracts."""
        try:
            # Load main OPPS schema
            opps_schema_path = Path(
                "cms_pricing/ingestion/contracts/cms_opps_v1.0.json"
            )
            if opps_schema_path.exists():
                with open(opps_schema_path, "r") as f:
                    self.opps_schema = json.load(f)
            else:
                logger.warning("OPPS schema contract not found, using defaults")
                self.opps_schema = self._create_default_opps_schema()

            # Load SI lookup schema
            si_schema_path = Path(
                "cms_pricing/ingestion/contracts/cms_opps_si_lookup_v1.0.json"
            )
            if si_schema_path.exists():
                with open(si_schema_path, "r") as f:
                    self.si_schema = json.load(f)
            else:
                logger.warning("SI lookup schema contract not found, using defaults")
                self.si_schema = self._create_default_si_schema()

            # Register schemas
            # Note: SchemaRegistry loads schemas automatically from files
            # We don't need to manually register them

        except Exception as e:
            logger.error("Failed to load schema contracts", error=str(e))
            raise

    def _create_default_opps_schema(self) -> Dict[str, Any]:
        """Create default OPPS schema contract."""
        return {
            "schema_id": "cms_opps_v1.0",
            "dataset_name": "cms_opps",
            "version": "1.0.0",
            "tables": {
                "opps_apc_payment": {
                    "description": "OPPS APC payment rates (Addendum A)",
                    "columns": {
                        "year": {"type": "integer", "nullable": False},
                        "quarter": {"type": "integer", "nullable": False},
                        "apc_code": {"type": "string", "nullable": False},
                        "apc_description": {"type": "string", "nullable": True},
                        "payment_rate_usd": {
                            "type": "decimal",
                            "precision": 12,
                            "scale": 3,
                            "nullable": False,
                        },
                        "relative_weight": {
                            "type": "decimal",
                            "precision": 8,
                            "scale": 4,
                            "nullable": True,
                        },
                        "packaging_flag": {"type": "string", "nullable": True},
                        "effective_from": {"type": "date", "nullable": False},
                        "effective_to": {"type": "date", "nullable": True},
                        "release_id": {"type": "string", "nullable": False},
                        "batch_id": {"type": "string", "nullable": False},
                    },
                    "primary_key": ["year", "quarter", "apc_code", "effective_from"],
                    "natural_key": ["apc_code", "effective_from"],
                },
                "opps_hcpcs_crosswalk": {
                    "description": "HCPCS to APC crosswalk with Status Indicators (Addendum B)",
                    "columns": {
                        "year": {"type": "integer", "nullable": False},
                        "quarter": {"type": "integer", "nullable": False},
                        "hcpcs_code": {"type": "string", "nullable": False},
                        "modifier": {"type": "string", "nullable": True},
                        "status_indicator": {"type": "string", "nullable": False},
                        "apc_code": {"type": "string", "nullable": True},
                        "payment_context": {"type": "string", "nullable": True},
                        "effective_from": {"type": "date", "nullable": False},
                        "effective_to": {"type": "date", "nullable": True},
                        "release_id": {"type": "string", "nullable": False},
                        "batch_id": {"type": "string", "nullable": False},
                    },
                    "primary_key": [
                        "year",
                        "quarter",
                        "hcpcs_code",
                        "modifier",
                        "effective_from",
                    ],
                    "natural_key": ["hcpcs_code", "modifier", "effective_from"],
                },
                "opps_rates_enriched": {
                    "description": "Enriched OPPS rates with wage index data",
                    "columns": {
                        "year": {"type": "integer", "nullable": False},
                        "quarter": {"type": "integer", "nullable": False},
                        "apc_code": {"type": "string", "nullable": False},
                        "ccn": {"type": "string", "nullable": True},
                        "cbsa_code": {"type": "string", "nullable": True},
                        "wage_index": {
                            "type": "decimal",
                            "precision": 6,
                            "scale": 3,
                            "nullable": True,
                        },
                        "payment_rate_usd": {
                            "type": "decimal",
                            "precision": 12,
                            "scale": 3,
                            "nullable": False,
                        },
                        "wage_adjusted_rate_usd": {
                            "type": "decimal",
                            "precision": 12,
                            "scale": 3,
                            "nullable": True,
                        },
                        "effective_from": {"type": "date", "nullable": False},
                        "effective_to": {"type": "date", "nullable": True},
                        "release_id": {"type": "string", "nullable": False},
                        "batch_id": {"type": "string", "nullable": False},
                    },
                    "primary_key": [
                        "year",
                        "quarter",
                        "apc_code",
                        "ccn",
                        "effective_from",
                    ],
                    "natural_key": ["apc_code", "ccn", "effective_from"],
                },
            },
            "business_rules": {
                "payment_rate_usd": {"min_value": 0, "max_value": 100000},
                "relative_weight": {"min_value": 0, "max_value": 1000},
                "wage_index": {"min_value": 0.3, "max_value": 2.0},
                "hcpcs_code": {"pattern": "^[A-Z0-9]{5}$"},
                "apc_code": {"pattern": "^[0-9]{4}$"},
                "status_indicator": {
                    "enum": [
                        "A",
                        "B",
                        "C",
                        "D",
                        "E",
                        "E1",
                        "E2",
                        "F",
                        "G",
                        "H",
                        "H1",
                        "J",
                        "J1",
                        "J2",
                        "K",
                        "K1",
                        "L",
                        "M",
                        "N",
                        "P",
                        "Q",
                        "Q1",
                        "Q2",
                        "Q3",
                        "Q4",
                        "R",
                        "S",
                        "S1",
                        "T",
                        "U",
                        "V",
                        "W",
                        "X",
                        "Y",
                        "Z",
                    ]
                },
            },
        }

    def _create_default_si_schema(self) -> Dict[str, Any]:
        """Create default SI lookup schema contract."""
        return {
            "schema_id": "cms_opps_si_lookup_v1.0",
            "dataset_name": "cms_opps_si_lookup",
            "version": "1.0.0",
            "tables": {
                "ref_si_lookup": {
                    "description": "Status Indicator lookup table",
                    "columns": {
                        "status_indicator": {"type": "string", "nullable": False},
                        "description": {"type": "string", "nullable": False},
                        "payment_category": {"type": "string", "nullable": True},
                        "effective_from": {"type": "date", "nullable": False},
                        "effective_to": {"type": "date", "nullable": True},
                    },
                    "primary_key": ["status_indicator", "effective_from"],
                    "natural_key": ["status_indicator", "effective_from"],
                }
            },
        }

    def _setup_validation_rules(self):
        """Setup validation rules for OPPS data."""
        self._validation_rules = [
            # Structural validation
            ValidationRule(
                name="required_files_present",
                description="Required Addendum A and B files must be present",
                validator_func=self._validate_required_files,
                severity="critical",
            ),
            ValidationRule(
                name="file_format_valid",
                description="Files must be in supported format (CSV, XLS, XLSX, TXT)",
                validator_func=self._validate_file_formats,
                severity="critical",
            ),
            # Schema validation
            ValidationRule(
                name="required_columns_present",
                description="Required columns must be present in all files",
                validator_func=self._validate_required_columns,
                severity="critical",
            ),
            ValidationRule(
                name="data_types_valid",
                description="Data types must match schema specifications",
                validator_func=self._validate_data_types,
                severity="critical",
            ),
            # Domain validation
            ValidationRule(
                name="hcpcs_code_format",
                description="HCPCS codes must be 5 characters (A-Z, 0-9)",
                validator_func=self._validate_hcpcs_codes,
                severity="critical",
            ),
            ValidationRule(
                name="apc_code_format",
                description="APC codes must be 4 digits",
                validator_func=self._validate_apc_codes,
                severity="critical",
            ),
            ValidationRule(
                name="status_indicator_valid",
                description="Status indicators must be valid values",
                validator_func=self._validate_status_indicators,
                severity="critical",
            ),
            ValidationRule(
                name="payment_rates_positive",
                description="Payment rates must be non-negative",
                validator_func=self._validate_payment_rates,
                severity="critical",
            ),
            # Cross-file validation
            ValidationRule(
                name="apc_referenced_in_b_exists_in_a",
                description="APC codes referenced in Addendum B must exist in Addendum A",
                validator_func=self._validate_apc_cross_reference,
                severity="critical",
            ),
            ValidationRule(
                name="hcpcs_exists_for_quarter",
                description="HCPCS codes must exist in HCPCS quarterly update",
                validator_func=self._validate_hcpcs_existence,
                severity="warning",
            ),
            # Temporal validation
            ValidationRule(
                name="no_overlapping_effective_ranges",
                description="No overlapping effective ranges for same HCPCS+modifier",
                validator_func=self._validate_temporal_uniqueness,
                severity="critical",
            ),
            # Statistical validation
            ValidationRule(
                name="row_count_drift",
                description="Row count must be within acceptable drift from previous quarter",
                validator_func=self._validate_row_count_drift,
                severity="warning",
            ),
            ValidationRule(
                name="rate_bounded_drift",
                description="Payment rate changes must be within acceptable bounds",
                validator_func=self._validate_rate_drift,
                severity="warning",
            ),
            ValidationRule(
                name="coverage_drift",
                description="HCPCS coverage must be within acceptable bounds",
                validator_func=self._validate_coverage_drift,
                severity="warning",
            ),
        ]

    def _setup_sla_specs(self):
        """Setup SLA specifications for DIS compliance."""
        self._sla_spec = SlaSpec(
            max_processing_time_hours=24,
            freshness_alert_hours=120,  # 5 days * 24 hours
            quality_threshold=0.99,
            availability_target=0.999,
        )

    def _setup_output_specs(self):
        """Setup output specifications for DIS compliance."""
        self._output_spec = OutputSpec(
            table_name="opps_curated",
            partition_columns=["year", "quarter", "effective_from"],
            output_format="parquet",
            compression="snappy",
            schema_evolution=True,
        )

    async def ingest_batch(self, batch_id: str) -> Dict[str, Any]:
        """
        Ingest a single OPPS batch following DIS 5-stage pipeline.

        Args:
            batch_id: Batch identifier (e.g., "opps_2025q1_r01")

        Returns:
            Ingestion results with metadata
        """
        logger.info("Starting OPPS batch ingestion", batch_id=batch_id)

        try:
            # Stage 1: Land - Discover and download files
            batch_info = await self._land_stage(batch_id)

            # Stage 2: Validate - Structural, schema, domain, and statistical validation
            validation_results = await self._validate_stage(batch_info)

            if not validation_results["passed"]:
                logger.error(
                    "Validation failed", batch_id=batch_id, results=validation_results
                )
                return {
                    "status": "failed",
                    "stage": "validate",
                    "batch_id": batch_id,
                    "validation_results": validation_results,
                    "timestamp": datetime.utcnow().isoformat(),
                }

            # Stage 3: Normalize - Canonicalize data
            normalized_data = await self._normalize_stage(batch_info)
            normalized_validation = self.validate_normalized_opps_data(normalized_data)
            if not normalized_validation["passed"]:
                logger.error(
                    "Normalized OPPS validation failed",
                    batch_id=batch_id,
                    results=normalized_validation,
                )
                return {
                    "status": "failed",
                    "stage": "normalize_validate",
                    "batch_id": batch_id,
                    "validation_results": normalized_validation,
                    "timestamp": datetime.utcnow().isoformat(),
                }

            # Stage 4: Enrich - Join with reference data
            enriched_data = await self._enrich_stage(normalized_data, batch_info)

            # Stage 5: Publish - Store in curated format
            publish_results = await self._publish_stage(enriched_data, batch_info)

            # Update observability metrics
            await self._update_observability_metrics(
                batch_info, validation_results, publish_results
            )

            logger.info(
                "OPPS batch ingestion completed successfully", batch_id=batch_id
            )

            return {
                "status": "success",
                "batch_id": batch_id,
                "stages_completed": [
                    "land",
                    "validate",
                    "normalize",
                    "enrich",
                    "publish",
                ],
                "validation_results": validation_results,
                "publish_results": publish_results,
                "timestamp": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.error("OPPS batch ingestion failed", batch_id=batch_id, error=str(e))

            # Quarantine failed batch
            await self._quarantine_batch(batch_id, str(e))

            return {
                "status": "failed",
                "batch_id": batch_id,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }

    async def _land_stage(self, batch_id: str) -> OPPSBatchInfo:
        """Land stage: Discover and download OPPS files."""
        logger.info("Starting land stage", batch_id=batch_id)

        # Parse batch ID to extract year, quarter, release
        year, quarter, release_num = self._parse_batch_id(batch_id)

        # Discover files for this quarter
        discovered_files = await self.scraper.discover_latest(quarters=1)

        # Filter for the specific quarter
        quarter_files = [
            f
            for f in discovered_files
            if f.metadata.get("year") == year and f.metadata.get("quarter") == quarter
        ]

        if not quarter_files:
            raise ValueError(f"No files found for quarter {year}Q{quarter}")

        # Download files (or reuse local sandbox files)
        downloaded_files = []
        for file_info in quarter_files:
            try:
                if file_info.local_path:
                    local_path_obj = file_info.local_path
                    if not isinstance(local_path_obj, Path):
                        local_path_obj = Path(local_path_obj)
                    if local_path_obj.exists():
                        file_info.local_path = local_path_obj
                        logger.info(
                            "Using local sandbox file",
                            file=file_info.filename,
                            path=str(local_path_obj),
                        )
                        downloaded_files.append(file_info)
                        continue
                local_path = await self.scraper.download_file(file_info)
                file_info.local_path = local_path
                logger.info(
                    "Downloaded OPPS file",
                    file=file_info.filename,
                    path=str(local_path),
                )
                downloaded_files.append(file_info)
            except Exception as e:
                logger.error(
                    "Failed to download file", file=file_info.filename, error=str(e)
                )
                raise

        # Calculate effective dates
        effective_from = self._calculate_effective_from(year, quarter)
        effective_to = self._calculate_effective_to(year, quarter)

        batch_info = OPPSBatchInfo(
            batch_id=batch_id,
            year=year,
            quarter=quarter,
            release_number=release_num,
            effective_from=effective_from,
            effective_to=effective_to,
            files=downloaded_files,
            discovered_at=datetime.utcnow(),
            downloaded_at=datetime.utcnow(),
        )

        # Generate manifest
        await self._generate_manifest(batch_info)

        logger.info(
            "Land stage completed",
            batch_id=batch_id,
            files_downloaded=len(downloaded_files),
        )
        return batch_info

    async def _validate_stage(self, batch_info: OPPSBatchInfo) -> Dict[str, Any]:
        """Validate stage: Structural, schema, domain, and statistical validation."""
        logger.info("Starting validate stage", batch_id=batch_info.batch_id)

        validation_results = {"passed": True, "rules": {}, "errors": [], "warnings": []}

        # Run all validation rules
        for rule in self._validation_rules:
            try:
                result = await rule.validator_func(batch_info)
                validation_results["rules"][rule.name] = result

                if not result["passed"]:
                    if rule.severity == "critical":
                        validation_results["passed"] = False
                        validation_results["errors"].extend(result["errors"])
                    else:
                        validation_results["warnings"].extend(result["errors"])

            except Exception as e:
                logger.error("Validation rule failed", rule=rule.name, error=str(e))
                validation_results["passed"] = False
                validation_results["errors"].append(
                    f"Rule {rule.name} failed: {str(e)}"
                )

        logger.info(
            "Validate stage completed",
            batch_id=batch_info.batch_id,
            passed=validation_results["passed"],
            errors=len(validation_results["errors"]),
            warnings=len(validation_results["warnings"]),
        )

        return validation_results

    async def _normalize_stage(
        self, batch_info: OPPSBatchInfo
    ) -> Dict[str, pd.DataFrame]:
        """Normalize stage: Canonicalize data formats and column names."""
        logger.info("Starting normalize stage", batch_id=batch_info.batch_id)

        normalized_data = {}

        for file_info in batch_info.files:
            try:
                # Read file based on type
                if file_info.file_type == "addendum_a":
                    df = await self._parse_addendum_a(file_info)
                    normalized_data[TABLE_OPPS_APC_PAYMENT] = df
                elif file_info.file_type == "addendum_b":
                    df = await self._parse_addendum_b(file_info)
                    normalized_data[TABLE_OPPS_HCPCS_CROSSWALK] = df
                elif file_info.file_type == "addendum_zip":
                    # Handle ZIP files containing multiple addenda
                    zip_data = await self._parse_zip_file(file_info)
                    normalized_data.update(zip_data)

            except Exception as e:
                logger.error(
                    "Failed to normalize file",
                    file=file_info.filename,
                    file_type=file_info.file_type,
                    error=str(e),
                )
                raise

        # Add common metadata
        for table_name, df in normalized_data.items():
            df["year"] = batch_info.year
            df["quarter"] = batch_info.quarter
            df["effective_from"] = batch_info.effective_from
            df["effective_to"] = batch_info.effective_to
            df["release_id"] = batch_info.batch_id
            df["batch_id"] = batch_info.batch_id

        logger.info(
            "Normalize stage completed",
            batch_id=batch_info.batch_id,
            tables_normalized=list(normalized_data.keys()),
        )

        return normalized_data

    async def _enrich_stage(
        self, normalized_data: Dict[str, pd.DataFrame], batch_info: OPPSBatchInfo
    ) -> Dict[str, pd.DataFrame]:
        """Enrich stage: Join with reference data (wage index, SI lookup)."""
        logger.info("Starting enrich stage", batch_id=batch_info.batch_id)

        enriched_data = normalized_data.copy()

        try:
            # Load reference data
            wage_index_data = await self._load_wage_index_data()
            si_lookup_data = await self._load_si_lookup_data()

            # Enrich APC payment data with wage index
            if TABLE_OPPS_APC_PAYMENT in enriched_data:
                enriched_data[
                    TABLE_OPPS_RATES_ENRICHED
                ] = await self._enrich_with_wage_index(
                    enriched_data[TABLE_OPPS_APC_PAYMENT], wage_index_data
                )

            # Enrich HCPCS crosswalk with SI descriptions
            if TABLE_OPPS_HCPCS_CROSSWALK in enriched_data:
                enriched_data[
                    TABLE_OPPS_HCPCS_CROSSWALK
                ] = await self._enrich_with_si_lookup(
                    enriched_data[TABLE_OPPS_HCPCS_CROSSWALK], si_lookup_data
                )

            logger.info("Enrich stage completed", batch_id=batch_info.batch_id)

        except Exception as e:
            logger.error(
                "Enrich stage failed", batch_id=batch_info.batch_id, error=str(e)
            )
            raise

        return enriched_data

    async def _publish_stage(
        self, enriched_data: Dict[str, pd.DataFrame], batch_info: OPPSBatchInfo
    ) -> Dict[str, Any]:
        """Publish stage: Store in curated format with metadata."""
        logger.info("Starting publish stage", batch_id=batch_info.batch_id)

        publish_results = {
            "tables_published": [],
            "records_published": 0,
            "files_generated": [],
            "table_artifacts": [],
            "file_checksums": {},
        }

        try:
            # Publish each table
            for table_name, df in enriched_data.items():
                # Apply CPT masking if enabled
                if self.cpt_masking_enabled:
                    df = self._apply_cpt_masking(df)

                # Store in curated format
                output_path = (
                    self.curated_dir / batch_info.batch_id / f"{table_name}.parquet"
                )
                output_path.parent.mkdir(parents=True, exist_ok=True)

                df.to_parquet(output_path, compression="snappy", index=False)
                file_checksum = self._sha256_file(output_path)
                row_hash = self._compute_row_content_hash(df, table_name)

                publish_results["tables_published"].append(table_name)
                publish_results["records_published"] += len(df)
                publish_results["files_generated"].append(str(output_path))
                publish_results["file_checksums"][table_name] = file_checksum
                publish_results["table_artifacts"].append(
                    {
                        "table": table_name,
                        "records": len(df),
                        "parquet_path": str(output_path),
                        "file_sha256": file_checksum,
                        "row_content_hash": row_hash,
                    }
                )

                logger.info(
                    "Published table",
                    table=table_name,
                    records=len(df),
                    path=str(output_path),
                )

            # Generate metadata
            await self._generate_curated_metadata(batch_info, publish_results)

            logger.info(
                "Publish stage completed",
                batch_id=batch_info.batch_id,
                tables=len(publish_results["tables_published"]),
                records=publish_results["records_published"],
            )

        except Exception as e:
            logger.error(
                "Publish stage failed", batch_id=batch_info.batch_id, error=str(e)
            )
            raise

        return publish_results

    def validate_normalized_opps_data(
        self,
        normalized_data: Dict[str, pd.DataFrame],
        *,
        min_apc_rows: int = 1,
        min_hcpcs_rows: int = 1,
    ) -> Dict[str, Any]:
        """Run production-readiness validation gates over normalized OPPS frames."""
        errors: List[str] = []
        warnings: List[str] = []

        apc_df = normalized_data.get(TABLE_OPPS_APC_PAYMENT)
        hcpcs_df = normalized_data.get(TABLE_OPPS_HCPCS_CROSSWALK)

        if apc_df is None or apc_df.empty:
            errors.append("opps_apc_payment is missing or empty")
        elif len(apc_df) < min_apc_rows:
            errors.append(
                f"opps_apc_payment row count {len(apc_df)} is below floor {min_apc_rows}"
            )

        if hcpcs_df is None or hcpcs_df.empty:
            errors.append("opps_hcpcs_crosswalk is missing or empty")
        elif len(hcpcs_df) < min_hcpcs_rows:
            errors.append(
                f"opps_hcpcs_crosswalk row count {len(hcpcs_df)} is below floor {min_hcpcs_rows}"
            )

        if apc_df is not None and not apc_df.empty:
            self._validate_required_frame_columns(
                apc_df,
                TABLE_OPPS_APC_PAYMENT,
                {"apc_code", "payment_rate_usd"},
                errors,
            )
            if "apc_code" in apc_df:
                bad_apc = apc_df[
                    ~apc_df["apc_code"].astype(str).str.match(r"^\d{4}$", na=False)
                ]
                if not bad_apc.empty:
                    errors.append(
                        f"opps_apc_payment has {len(bad_apc)} invalid APC codes"
                    )
                duplicates = apc_df["apc_code"].duplicated().sum()
                if duplicates:
                    errors.append(
                        f"opps_apc_payment has {duplicates} duplicate APC natural keys"
                    )
            if "payment_rate_usd" in apc_df:
                missing_rate_rows = apc_df[apc_df["payment_rate_usd"].isna()]
                if not missing_rate_rows.empty:
                    missing_apcs = set(
                        missing_rate_rows["apc_code"].dropna().astype(str)
                    )
                    separately_payable_missing_apcs = set()
                    if (
                        hcpcs_df is not None
                        and not hcpcs_df.empty
                        and {"apc_code", "status_indicator"}.issubset(hcpcs_df.columns)
                    ):
                        refs = hcpcs_df[
                            hcpcs_df["apc_code"].astype(str).isin(missing_apcs)
                        ]
                        separately_payable_missing_apcs = set(
                            refs[
                                refs["status_indicator"].map(
                                    lambda value: self._status_requires_apc_payment_rate(
                                        str(value)
                                    )
                                )
                            ]["apc_code"]
                            .dropna()
                            .astype(str)
                        )
                    else:
                        separately_payable_missing_apcs = missing_apcs

                    if separately_payable_missing_apcs:
                        errors.append(
                            "opps_apc_payment has missing payment rates for separately payable APCs: "
                            f"{sorted(separately_payable_missing_apcs)}"
                        )
                    else:
                        warnings.append(
                            "opps_apc_payment has blank payment rates only for non-separately payable APCs"
                        )
                negative_rates = (
                    apc_df["payment_rate_usd"]
                    .dropna()
                    .map(lambda value: Decimal(str(value)) < 0)
                    .sum()
                )
                if negative_rates:
                    errors.append(
                        f"opps_apc_payment has {negative_rates} negative payment rates"
                    )
            if "relative_weight" in apc_df and apc_df["relative_weight"].isna().any():
                warnings.append(
                    "opps_apc_payment has CMS rows with blank relative_weight; accepted for source fidelity"
                )

        if hcpcs_df is not None and not hcpcs_df.empty:
            self._validate_required_frame_columns(
                hcpcs_df,
                TABLE_OPPS_HCPCS_CROSSWALK,
                {"hcpcs_code", "status_indicator"},
                errors,
            )
            if "hcpcs_code" in hcpcs_df:
                bad_hcpcs = hcpcs_df[
                    ~hcpcs_df["hcpcs_code"]
                    .astype(str)
                    .str.match(r"^[A-Z0-9]{5}$", na=False)
                ]
                if not bad_hcpcs.empty:
                    errors.append(
                        f"opps_hcpcs_crosswalk has {len(bad_hcpcs)} invalid HCPCS codes"
                    )
            if {"hcpcs_code", "modifier"}.issubset(hcpcs_df.columns):
                duplicates = (
                    hcpcs_df[["hcpcs_code", "modifier"]].astype(str).duplicated().sum()
                )
                if duplicates:
                    errors.append(
                        f"opps_hcpcs_crosswalk has {duplicates} duplicate HCPCS/modifier natural keys"
                    )
            if "status_indicator" in hcpcs_df:
                allowed = self._status_indicator_domain()
                unknown = sorted(
                    set(hcpcs_df["status_indicator"].dropna().astype(str)) - allowed
                )
                if unknown:
                    errors.append(
                        f"opps_hcpcs_crosswalk has unknown status indicators: {unknown}"
                    )
            if "apc_code" in hcpcs_df and apc_df is not None and not apc_df.empty:
                known_apcs = set(apc_df["apc_code"].dropna().astype(str))
                referenced = set(hcpcs_df["apc_code"].dropna().astype(str))
                missing = sorted(referenced - known_apcs)
                if missing:
                    errors.append(
                        f"Addendum B references {len(missing)} APCs missing from Addendum A"
                    )

        return {
            "passed": not errors,
            "errors": errors,
            "warnings": warnings,
            "row_counts": {
                TABLE_OPPS_APC_PAYMENT: 0 if apc_df is None else int(len(apc_df)),
                TABLE_OPPS_HCPCS_CROSSWALK: 0
                if hcpcs_df is None
                else int(len(hcpcs_df)),
            },
        }

    def persist_normalized_opps_data(
        self,
        session,
        normalized_data: Dict[str, pd.DataFrame],
        batch_info: OPPSBatchInfo,
        *,
        replace_existing: bool = True,
    ) -> Dict[str, Any]:
        """Persist normalized OPPS Addendum A/B rows and derived SI lookup rows."""
        validation = self.validate_normalized_opps_data(normalized_data)
        if not validation["passed"]:
            raise ValueError(
                f"Normalized OPPS data failed validation: {validation['errors']}"
            )

        if replace_existing:
            session.query(OPPSAPCPayment).filter(
                OPPSAPCPayment.release_id == batch_info.batch_id
            ).delete()
            session.query(OPPSHCPCSCrosswalk).filter(
                OPPSHCPCSCrosswalk.release_id == batch_info.batch_id
            ).delete()
            session.query(RefSILookup).filter(
                RefSILookup.effective_from == batch_info.effective_from
            ).delete()
            session.flush()

        today = date.today()
        apc_rows = []
        for row in normalized_data[TABLE_OPPS_APC_PAYMENT].to_dict("records"):
            apc_rows.append(
                OPPSAPCPayment(
                    year=batch_info.year,
                    quarter=batch_info.quarter,
                    effective_from=batch_info.effective_from,
                    effective_to=batch_info.effective_to,
                    apc_code=row["apc_code"],
                    apc_description=row.get("apc_description"),
                    payment_rate_usd=row["payment_rate_usd"],
                    relative_weight=row.get("relative_weight"),
                    packaging_flag=row.get("packaging_flag"),
                    release_id=batch_info.batch_id,
                    batch_id=batch_info.batch_id,
                    created_at=today,
                    updated_at=today,
                )
            )

        hcpcs_rows = []
        for row in normalized_data[TABLE_OPPS_HCPCS_CROSSWALK].to_dict("records"):
            hcpcs_rows.append(
                OPPSHCPCSCrosswalk(
                    year=batch_info.year,
                    quarter=batch_info.quarter,
                    effective_from=batch_info.effective_from,
                    effective_to=batch_info.effective_to,
                    hcpcs_code=row["hcpcs_code"],
                    modifier=row.get("modifier"),
                    status_indicator=row["status_indicator"],
                    apc_code=row.get("apc_code"),
                    payment_context=row.get("payment_context"),
                    release_id=batch_info.batch_id,
                    batch_id=batch_info.batch_id,
                    created_at=today,
                    updated_at=today,
                )
            )

        si_rows = self._build_si_lookup_rows(
            normalized_data[TABLE_OPPS_HCPCS_CROSSWALK],
            batch_info=batch_info,
            today=today,
        )

        session.add_all(apc_rows + hcpcs_rows + si_rows)
        session.flush()
        return {
            "tables_loaded": {
                TABLE_OPPS_APC_PAYMENT: len(apc_rows),
                TABLE_OPPS_HCPCS_CROSSWALK: len(hcpcs_rows),
                "ref_si_lookup": len(si_rows),
            },
            "validation": validation,
        }

    def register_opps_snapshots(
        self,
        session,
        batch_info: OPPSBatchInfo,
        *,
        manifest_url: Optional[str] = None,
        source_digest: Optional[str] = None,
        allow_overwrite: bool = True,
    ) -> List[Dict[str, Any]]:
        """Register OPPS aggregate and table-level snapshots using CMS effective dates."""
        digest_seed = source_digest or batch_info.batch_id
        snapshot_specs = {
            "OPPS": digest_seed,
            TABLE_OPPS_APC_PAYMENT: f"{digest_seed}:{TABLE_OPPS_APC_PAYMENT}",
            TABLE_OPPS_HCPCS_CROSSWALK: f"{digest_seed}:{TABLE_OPPS_HCPCS_CROSSWALK}",
            "ref_si_lookup": f"{digest_seed}:ref_si_lookup",
        }
        service = DatasetSnapshotService(session)
        registered = []
        for dataset_id, seed in snapshot_specs.items():
            digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
            snapshot = service.register_snapshot(
                dataset_id=dataset_id,
                release_id=batch_info.batch_id,
                digest=digest,
                effective_from=batch_info.effective_from,
                effective_to=batch_info.effective_to,
                manifest_url=manifest_url,
                allow_overwrite=allow_overwrite,
                autocommit=False,
            )
            registered.append(snapshot.to_dict())
        session.flush()
        return registered

    def _build_si_lookup_rows(
        self,
        hcpcs_df: pd.DataFrame,
        *,
        batch_info: OPPSBatchInfo,
        today: date,
    ) -> List[RefSILookup]:
        rows = []
        for status_indicator in sorted(
            hcpcs_df["status_indicator"].dropna().astype(str).unique()
        ):
            rows.append(
                RefSILookup(
                    status_indicator=status_indicator,
                    description=f"OPPS status indicator {status_indicator} observed in {batch_info.batch_id}",
                    payment_category=self._payment_category_for_status(
                        status_indicator
                    ),
                    effective_from=batch_info.effective_from,
                    effective_to=batch_info.effective_to,
                    created_at=today,
                    updated_at=today,
                )
            )
        return rows

    def _payment_category_for_status(self, status_indicator: str) -> str:
        if status_indicator in {
            "B",
            "C",
            "F",
            "H",
            "H1",
            "K1",
            "L",
            "N",
            "Q1",
            "Q2",
            "Q3",
            "Q4",
            "J1",
            "J2",
        }:
            return "Packaged"
        if status_indicator in {"A", "S", "S1", "T", "V", "U", "K", "G", "P", "R"}:
            return "Separately Payable"
        if status_indicator in {"E1", "E2", "M", "Y"}:
            return "Not Payable"
        return "Other"

    def _status_requires_apc_payment_rate(self, status_indicator: str) -> bool:
        return status_indicator in {"A", "G", "K", "P", "R", "S", "S1", "T", "U", "V"}

    def _status_indicator_domain(self) -> set[str]:
        try:
            return set(
                self.opps_schema["tables"][TABLE_OPPS_HCPCS_CROSSWALK]["columns"][
                    "status_indicator"
                ]["validation"]["enum"]
            )
        except Exception:
            return {
                "A",
                "B",
                "C",
                "E1",
                "E2",
                "F",
                "G",
                "H",
                "H1",
                "J1",
                "J2",
                "K",
                "K1",
                "L",
                "M",
                "N",
                "P",
                "Q1",
                "Q2",
                "Q3",
                "Q4",
                "R",
                "S",
                "S1",
                "T",
                "U",
                "V",
                "Y",
            }

    def _validate_required_frame_columns(
        self,
        df: pd.DataFrame,
        table_name: str,
        required: set[str],
        errors: List[str],
    ) -> None:
        missing = sorted(required - set(df.columns))
        if missing:
            errors.append(f"{table_name} missing required columns: {missing}")

    def _parse_batch_id(self, batch_id: str) -> Tuple[int, int, int]:
        """Parse batch ID to extract year, quarter, release number."""
        # Format: opps_YYYYqN_rNN
        import re

        match = re.match(r"opps_(\d{4})q(\d)_r(\d+)", batch_id)
        if not match:
            raise ValueError(f"Invalid batch ID format: {batch_id}")

        year = int(match.group(1))
        quarter = int(match.group(2))
        release_num = int(match.group(3))

        return year, quarter, release_num

    def _calculate_effective_from(self, year: int, quarter: int) -> date:
        """Calculate effective from date for quarter."""
        quarter_starts = {
            1: date(year, 1, 1),
            2: date(year, 4, 1),
            3: date(year, 7, 1),
            4: date(year, 10, 1),
        }
        return quarter_starts[quarter]

    def _calculate_effective_to(self, year: int, quarter: int) -> date:
        """Calculate effective to date for quarter."""
        quarter_ends = {
            1: date(year, 3, 31),
            2: date(year, 6, 30),
            3: date(year, 9, 30),
            4: date(year, 12, 31),
        }
        return quarter_ends[quarter]

    async def _generate_manifest(self, batch_info: OPPSBatchInfo):
        """Generate manifest file for the batch."""
        manifest = {
            "batch_id": batch_info.batch_id,
            "year": batch_info.year,
            "quarter": batch_info.quarter,
            "quarter_vintage": self._quarter_to_letter(batch_info.quarter),
            "release_number": batch_info.release_number,
            "effective_from": batch_info.effective_from.isoformat(),
            "effective_to": batch_info.effective_to.isoformat()
            if batch_info.effective_to
            else None,
            "discovered_at": batch_info.discovered_at.isoformat(),
            "downloaded_at": batch_info.downloaded_at.isoformat()
            if batch_info.downloaded_at
            else None,
            "files": [
                {
                    "filename": f.filename,
                    "file_type": f.file_type,
                    "url": f.url,
                    "local_path": str(f.local_path) if f.local_path else None,
                    "checksum": f.checksum,
                    "size_bytes": f.local_path.stat().st_size
                    if f.local_path and f.local_path.exists()
                    else None,
                    "metadata": f.metadata,
                }
                for f in batch_info.files
            ],
            "ingester_version": "1.0.0",
            "dis_compliance": "v1.0",
            "qts_compliance": "v1.0",
        }

        manifest_path = self.raw_dir / batch_info.batch_id / "manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        logger.info("Generated manifest", path=str(manifest_path))

    # Validation methods (implemented as async methods for consistency)
    async def _validate_required_files(
        self, batch_info: OPPSBatchInfo
    ) -> Dict[str, Any]:
        """Validate that required files are present."""
        file_types = [f.file_type for f in batch_info.files]
        profile_override = os.getenv("OPPS_ADDENDA_PROFILE")
        profile = self.artifact_profile_service.resolve(
            "opps",
            release_id=batch_info.batch_id,
            profile_override=profile_override,
            sandbox_mode=bool(self.local_sample_dir),
        )
        result = profile.validate(file_types)
        return {
            "passed": result.passed,
            "errors": result.errors,
            "warnings": result.warnings,
            "file_types_found": file_types,
            "required_types": result.required,
            "optional_types": result.optional,
            "profile": result.profile_name,
        }

    async def _validate_file_formats(self, batch_info: OPPSBatchInfo) -> Dict[str, Any]:
        """Validate file formats are supported."""
        supported_extensions = {".csv", ".xls", ".xlsx", ".txt", ".zip"}
        errors = []

        for file_info in batch_info.files:
            if file_info.local_path and file_info.local_path.exists():
                ext = file_info.local_path.suffix.lower()
                if ext not in supported_extensions:
                    errors.append(
                        f"Unsupported file format: {ext} for {file_info.filename}"
                    )

        return {"passed": len(errors) == 0, "errors": errors}

    async def _validate_required_columns(
        self, batch_info: OPPSBatchInfo
    ) -> Dict[str, Any]:
        """Validate required columns are present."""
        # This would be implemented based on the actual file parsing
        # For now, return a placeholder
        return {"passed": True, "errors": []}

    async def _validate_data_types(self, batch_info: OPPSBatchInfo) -> Dict[str, Any]:
        """Validate data types match schema."""
        # This would be implemented based on the actual file parsing
        # For now, return a placeholder
        return {"passed": True, "errors": []}

    async def _validate_hcpcs_codes(self, batch_info: OPPSBatchInfo) -> Dict[str, Any]:
        """Validate HCPCS code format."""
        # This would be implemented based on the actual file parsing
        # For now, return a placeholder
        return {"passed": True, "errors": []}

    async def _validate_apc_codes(self, batch_info: OPPSBatchInfo) -> Dict[str, Any]:
        """Validate APC code format."""
        # This would be implemented based on the actual file parsing
        # For now, return a placeholder
        return {"passed": True, "errors": []}

    async def _validate_status_indicators(
        self, batch_info: OPPSBatchInfo
    ) -> Dict[str, Any]:
        """Validate status indicators."""
        # This would be implemented based on the actual file parsing
        # For now, return a placeholder
        return {"passed": True, "errors": []}

    async def _validate_payment_rates(
        self, batch_info: OPPSBatchInfo
    ) -> Dict[str, Any]:
        """Validate payment rates are non-negative."""
        # This would be implemented based on the actual file parsing
        # For now, return a placeholder
        return {"passed": True, "errors": []}

    async def _validate_apc_cross_reference(
        self, batch_info: OPPSBatchInfo
    ) -> Dict[str, Any]:
        """Validate APC codes referenced in B exist in A."""
        # This would be implemented based on the actual file parsing
        # For now, return a placeholder
        return {"passed": True, "errors": []}

    async def _validate_hcpcs_existence(
        self, batch_info: OPPSBatchInfo
    ) -> Dict[str, Any]:
        """Validate HCPCS codes exist in quarterly update."""
        # This would be implemented based on the actual file parsing
        # For now, return a placeholder
        return {"passed": True, "errors": []}

    async def _validate_temporal_uniqueness(
        self, batch_info: OPPSBatchInfo
    ) -> Dict[str, Any]:
        """Validate no overlapping effective ranges."""
        # This would be implemented based on the actual file parsing
        # For now, return a placeholder
        return {"passed": True, "errors": []}

    async def _validate_row_count_drift(
        self, batch_info: OPPSBatchInfo
    ) -> Dict[str, Any]:
        """Validate row count drift from previous quarter."""
        # This would be implemented based on the actual file parsing
        # For now, return a placeholder
        return {"passed": True, "errors": []}

    async def _validate_rate_drift(self, batch_info: OPPSBatchInfo) -> Dict[str, Any]:
        """Validate payment rate drift from previous quarter."""
        # This would be implemented based on the actual file parsing
        # For now, return a placeholder
        return {"passed": True, "errors": []}

    async def _validate_coverage_drift(
        self, batch_info: OPPSBatchInfo
    ) -> Dict[str, Any]:
        """Validate HCPCS coverage drift from previous quarter."""
        # This would be implemented based on the actual file parsing
        # For now, return a placeholder
        return {"passed": True, "errors": []}

    async def _parse_addendum_a(self, file_info: ScrapedFileInfo) -> pd.DataFrame:
        """Parse Addendum A file."""
        path = self._require_local_path(file_info)
        raw = self._read_opps_table(path, header_markers=("APC",))
        df = self._canonicalize_addendum_a(raw)
        logger.info("Parsed OPPS Addendum A", file=file_info.filename, rows=len(df))
        return df

    async def _parse_addendum_b(self, file_info: ScrapedFileInfo) -> pd.DataFrame:
        """Parse Addendum B file."""
        path = self._require_local_path(file_info)
        raw = self._read_opps_table(path, header_markers=("HCPCS Code", "HCPCS"))
        df = self._canonicalize_addendum_b(raw)
        logger.info("Parsed OPPS Addendum B", file=file_info.filename, rows=len(df))
        return df

    async def _parse_zip_file(
        self, file_info: ScrapedFileInfo
    ) -> Dict[str, pd.DataFrame]:
        """Parse ZIP file containing multiple addenda."""
        path = self._require_local_path(file_info)
        parsed: Dict[str, pd.DataFrame] = {}
        extract_dir = self.stage_dir / "zip_extract" / path.stem
        extract_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if not name.endswith("/")]
            for addendum, table_name, parser in (
                ("addendum a", TABLE_OPPS_APC_PAYMENT, self._parse_addendum_a),
                ("addendum b", TABLE_OPPS_HCPCS_CROSSWALK, self._parse_addendum_b),
            ):
                candidates = [
                    name
                    for name in names
                    if addendum in Path(name).name.lower()
                    and Path(name).suffix.lower() in {".csv", ".xlsx", ".xls"}
                ]
                if not candidates:
                    continue
                candidates.sort(
                    key=lambda name: (Path(name).suffix.lower() != ".csv", name)
                )
                member = candidates[0]
                target_path = extract_dir / Path(member).name
                with archive.open(member) as source, open(target_path, "wb") as target:
                    target.write(source.read())
                nested_info = ScrapedFileInfo(
                    url=file_info.url,
                    filename=Path(member).name,
                    file_type=addendum.replace(" ", "_"),
                    batch_id=file_info.batch_id,
                    discovered_at=file_info.discovered_at,
                    source_page=file_info.source_page,
                    metadata=file_info.metadata,
                    local_path=target_path,
                    checksum=None,
                    downloaded_at=file_info.downloaded_at,
                )
                parsed[table_name] = await parser(nested_info)

        if not parsed:
            raise ValueError(f"No supported Addendum A/B files found in ZIP: {path}")
        return parsed

    def _require_local_path(self, file_info: ScrapedFileInfo) -> Path:
        if not file_info.local_path:
            raise ValueError(f"Missing local path for {file_info.filename}")
        path = Path(file_info.local_path)
        if not path.exists():
            raise FileNotFoundError(f"OPPS source file does not exist: {path}")
        return path

    def _read_opps_table(
        self, path: Path, header_markers: Tuple[str, ...]
    ) -> pd.DataFrame:
        suffix = path.suffix.lower()
        if suffix == ".csv":
            preview, encoding = self._read_csv_with_encoding(
                path, header=None, nrows=40
            )
            header_index = self._find_header_index(preview, header_markers)
            df, _ = self._read_csv_with_encoding(
                path, header=header_index, encoding=encoding
            )
        elif suffix in {".xlsx", ".xls"}:
            preview = pd.read_excel(path, header=None, dtype=str, nrows=40)
            header_index = self._find_header_index(preview, header_markers)
            df = pd.read_excel(path, header=header_index, dtype=str)
        else:
            raise ValueError(f"Unsupported OPPS file format: {path.suffix}")

        df = df.dropna(axis=1, how="all")
        df = df.rename(columns=lambda value: self._clean_header(value))
        df = df.loc[:, [column for column in df.columns if column]]
        return df.dropna(how="all")

    def _read_csv_with_encoding(
        self,
        path: Path,
        header: Optional[int],
        nrows: Optional[int] = None,
        encoding: Optional[str] = None,
    ) -> Tuple[pd.DataFrame, str]:
        encodings = [encoding] if encoding else ["utf-8-sig", "cp1252", "latin1"]
        last_error: Optional[Exception] = None
        for candidate in encodings:
            if not candidate:
                continue
            try:
                return (
                    pd.read_csv(
                        path,
                        header=header,
                        dtype=str,
                        nrows=nrows,
                        engine="python",
                        encoding=candidate,
                    ),
                    candidate,
                )
            except UnicodeDecodeError as exc:
                last_error = exc
                if encoding:
                    for fallback in ("cp1252", "latin1"):
                        if fallback not in encodings:
                            encodings.append(fallback)
        if last_error:
            raise last_error
        raise ValueError(f"No CSV encoding candidates configured for {path}")

    def _find_header_index(
        self, preview: pd.DataFrame, header_markers: Tuple[str, ...]
    ) -> int:
        markers = {self._clean_header(marker) for marker in header_markers}
        for index, row in preview.iterrows():
            values = {self._clean_header(value) for value in row.tolist()}
            if markers & values:
                return int(index)
        raise ValueError(f"Could not find OPPS header row containing {sorted(markers)}")

    def _canonicalize_addendum_a(self, df: pd.DataFrame) -> pd.DataFrame:
        source = self._rename_known_columns(
            df,
            {
                "APC": "apc_code",
                "Group Title": "apc_description",
                "Relative Weight": "relative_weight",
                "Payment Rate": "payment_rate_usd",
                "SI": "status_indicator",
            },
        )
        required = {"apc_code", "payment_rate_usd"}
        self._require_columns(source, required, "Addendum A")

        result = pd.DataFrame()
        result["apc_code"] = source["apc_code"].map(self._clean_code).str.zfill(4)
        result["apc_description"] = source.get(
            "apc_description", pd.Series(index=source.index, dtype=object)
        ).map(self._clean_optional_text)
        result["payment_rate_usd"] = source["payment_rate_usd"].map(self._parse_decimal)
        result["relative_weight"] = source.get(
            "relative_weight", pd.Series(index=source.index, dtype=object)
        ).map(self._parse_decimal)
        result["packaging_flag"] = None
        return result[result["apc_code"].str.match(r"^\d{4}$", na=False)].reset_index(
            drop=True
        )

    def _canonicalize_addendum_b(self, df: pd.DataFrame) -> pd.DataFrame:
        source = self._rename_known_columns(
            df,
            {
                "HCPCS Code": "hcpcs_code",
                "HCPCS": "hcpcs_code",
                "Short Descriptor": "payment_context",
                "SI": "status_indicator",
                "APC": "apc_code",
            },
        )
        required = {"hcpcs_code", "status_indicator"}
        self._require_columns(source, required, "Addendum B")

        result = pd.DataFrame()
        result["hcpcs_code"] = source["hcpcs_code"].map(self._clean_code).str.upper()
        result["modifier"] = None
        result["status_indicator"] = (
            source["status_indicator"].map(self._clean_code).str.upper()
        )
        apc_source = source.get("apc_code", pd.Series(index=source.index, dtype=object))
        result["apc_code"] = apc_source.map(self._clean_optional_apc)
        context = source.get(
            "payment_context", pd.Series(index=source.index, dtype=object)
        )
        result["payment_context"] = context.map(self._clean_optional_text)
        return result[
            result["hcpcs_code"].str.match(r"^[A-Z0-9]{5}$", na=False)
        ].reset_index(drop=True)

    def _rename_known_columns(
        self, df: pd.DataFrame, mapping: Dict[str, str]
    ) -> pd.DataFrame:
        normalized = {self._clean_header(key): value for key, value in mapping.items()}
        return df.rename(
            columns={
                column: normalized.get(self._clean_header(column), column)
                for column in df.columns
            }
        )

    def _require_columns(
        self, df: pd.DataFrame, required: set[str], source_name: str
    ) -> None:
        missing = sorted(required - set(df.columns))
        if missing:
            raise ValueError(f"Missing required columns in {source_name}: {missing}")

    def _clean_header(self, value: Any) -> str:
        if value is None or pd.isna(value):
            return ""
        return re.sub(r"\s+", " ", str(value).replace("\ufeff", "").strip())

    def _clean_code(self, value: Any) -> Optional[str]:
        text = self._clean_optional_text(value)
        return text.replace(" ", "") if text else None

    def _clean_optional_apc(self, value: Any) -> Optional[str]:
        text = self._clean_code(value)
        if not text or text in {".", "N/A"}:
            return None
        return text.zfill(4) if text.isdigit() else text

    def _clean_optional_text(self, value: Any) -> Optional[str]:
        if value is None or pd.isna(value):
            return None
        text = re.sub(r"\s+", " ", str(value).strip())
        return text or None

    def _parse_decimal(self, value: Any) -> Optional[Decimal]:
        text = self._clean_optional_text(value)
        if not text or text in {".", "-", "N/A"}:
            return None
        cleaned = re.sub(r"[^0-9.\-]", "", text)
        if not cleaned or cleaned in {".", "-"}:
            return None
        try:
            return Decimal(cleaned)
        except InvalidOperation as exc:
            raise ValueError(f"Invalid OPPS decimal value: {value!r}") from exc

    # Enrichment methods
    async def _load_wage_index_data(self) -> pd.DataFrame:
        """Load wage index reference data."""
        # This would load from reference tables
        # For now, return empty DataFrame
        return pd.DataFrame(columns=["ccn", "cbsa_code", "wage_index"])

    async def _load_si_lookup_data(self) -> pd.DataFrame:
        """Load SI lookup reference data."""
        # This would load from reference tables
        # For now, return empty DataFrame
        return pd.DataFrame(
            columns=["status_indicator", "description", "payment_category"]
        )

    async def _enrich_with_wage_index(
        self, apc_data: pd.DataFrame, wage_data: pd.DataFrame
    ) -> pd.DataFrame:
        """Enrich APC data with wage index."""
        # This would implement wage index enrichment
        # For now, return the original data
        return apc_data

    async def _enrich_with_si_lookup(
        self, hcpcs_data: pd.DataFrame, si_data: pd.DataFrame
    ) -> pd.DataFrame:
        """Enrich HCPCS data with SI lookup."""
        # This would implement SI lookup enrichment
        # For now, return the original data
        return hcpcs_data

    # CPT masking
    def _apply_cpt_masking(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply CPT masking to external outputs."""
        if not self.cpt_masking_enabled:
            return df

        # Mask CPT descriptors in external outputs
        # This would implement actual masking logic
        # For now, return the original data
        return df

    # Utility methods
    async def _quarantine_batch(self, batch_id: str, error_message: str):
        """Quarantine failed batch."""
        quarantine_path = self.quarantine_dir / batch_id
        quarantine_path.mkdir(parents=True, exist_ok=True)

        quarantine_info = {
            "batch_id": batch_id,
            "quarantined_at": datetime.utcnow().isoformat(),
            "error_message": error_message,
            "quarantine_reason": "ingestion_failure",
        }

        with open(quarantine_path / "quarantine_info.json", "w") as f:
            json.dump(quarantine_info, f, indent=2)

        logger.info("Batch quarantined", batch_id=batch_id, reason=error_message)

    async def _update_observability_metrics(
        self, batch_info: OPPSBatchInfo, validation_results: Dict, publish_results: Dict
    ):
        """Update observability metrics."""
        # This would update the 5-pillar observability metrics
        # For now, just log the completion
        logger.info("Observability metrics updated", batch_id=batch_info.batch_id)

    async def _generate_curated_metadata(
        self, batch_info: OPPSBatchInfo, publish_results: Dict
    ):
        """Generate curated metadata."""
        quarter_vintage = self._quarter_to_letter(batch_info.quarter)
        source_files = []
        for f in batch_info.files:
            local_path = str(f.local_path) if f.local_path else None
            size_bytes = (
                f.local_path.stat().st_size
                if f.local_path and f.local_path.exists()
                else None
            )
            source_files.append(
                {
                    "filename": f.filename,
                    "file_type": f.file_type,
                    "source_url": f.url,
                    "source_file_sha256": f.metadata.get("sha256")
                    if f.metadata
                    else f.checksum,
                    "checksum": f.checksum,
                    "size_bytes": size_bytes,
                    "local_path": local_path,
                    "metadata": f.metadata,
                }
            )

        metadata = {
            "batch_id": batch_info.batch_id,
            "release_id": batch_info.batch_id,
            "product_year": batch_info.year,
            "year": batch_info.year,
            "quarter": batch_info.quarter,
            "quarter_vintage": quarter_vintage,
            "vintage_date": batch_info.effective_from.isoformat(),
            "effective_from": batch_info.effective_from.isoformat(),
            "effective_to": batch_info.effective_to.isoformat()
            if batch_info.effective_to
            else None,
            "published_at": datetime.utcnow().isoformat(),
            "tables_published": publish_results["tables_published"],
            "records_published": publish_results["records_published"],
            "files_generated": publish_results["files_generated"],
            "table_artifacts": publish_results.get("table_artifacts", []),
            "file_checksums": publish_results.get("file_checksums", {}),
            "source_files": source_files,
            "cpt_masking_enabled": self.cpt_masking_enabled,
            "ingester_version": "1.0.0",
            "dis_compliance": "v1.0",
            "qts_compliance": "v1.0",
        }

        metadata_path = self.curated_dir / batch_info.batch_id / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info("Generated curated metadata", path=str(metadata_path))

    def _quarter_to_letter(self, quarter: int) -> str:
        """Map quarter integer to CMS letter vintage."""
        mapping = {1: "A", 2: "B", 3: "C", 4: "D"}
        return mapping.get(quarter, "A")

    def _sha256_file(self, path: Path) -> Optional[str]:
        """Return SHA256 for a file path, if it exists."""
        if not path.exists():
            return None
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(8192), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _compute_row_content_hash(
        self, df: pd.DataFrame, table_name: str
    ) -> Optional[str]:
        """Compute deterministic row-content hash for a dataframe."""
        try:
            if df.empty:
                return hashlib.sha256(b"").hexdigest()
            normalized = df.sort_index(axis=1)
            row_hashes = pd.util.hash_pandas_object(
                normalized, index=True
            ).values.tobytes()
            return hashlib.sha256(row_hashes).hexdigest()
        except Exception as exc:
            logger.warning(
                "Unable to compute row content hash", table=table_name, error=str(exc)
            )
            return None


# CLI interface
async def main():
    """CLI entry point for OPPS ingester."""
    import argparse

    parser = argparse.ArgumentParser(description="CMS OPPS Ingester")
    parser.add_argument(
        "--batch-id", required=True, help="Batch ID to ingest (e.g., opps_2025q1_r01)"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data"), help="Output directory"
    )
    parser.add_argument("--database-url", help="Database URL")
    parser.add_argument(
        "--cpt-masking", action="store_true", default=True, help="Enable CPT masking"
    )

    args = parser.parse_args()

    ingester = OPPSIngestor(
        output_dir=args.output_dir,
        database_url=args.database_url,
        cpt_masking_enabled=args.cpt_masking,
    )

    result = await ingester.ingest_batch(args.batch_id)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
