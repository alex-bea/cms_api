"""
Schema Service
--------------

Phase 2 Refactoring Context:
    - Step 1: Schema registration extraction
      • Plan: artifacts/phase2_completion_plan.md (§Step 1)
      • Verification: artifacts/phase2_regression_test_results.md

This module provides bootstrap and helper utilities around the shared
SchemaRegistry so individual ingestors do not each carry their own registration
logic. The service coordinates schema registration to honour guardrail #3
(register once, preserve caching semantics) while keeping the contracts close
to the dataset concern.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, List
import structlog

from ..contracts.schema_registry import SchemaContract, ColumnSpec

logger = structlog.get_logger()


class SchemaService:
    """Coordinates schema registry bootstrap and caching."""

    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name
        self._rvu_bootstrapped = False

    # Phase 2 Step 1: Schema registration extraction
    # See: artifacts/phase2_completion_plan.md (§Step 1)
    def bootstrap_rvu_schemas(self, registry: Any) -> None:
        """
        Register all RVU schema contracts with the provided registry.
        Safe to call multiple times; subsequent calls are no-ops.
        """
        if self.dataset_name not in {"cms_rvu", "rvu"}:
            logger.debug(
                "Skipping RVU schema bootstrap for non-RVU dataset",
                dataset=self.dataset_name,
            )
            return

        if self._rvu_bootstrapped:
            logger.debug("RVU schemas already bootstrapped", dataset=self.dataset_name)
            return

        schemas = self._build_rvu_schema_contracts()
        registered = 0
        for schema in schemas:
            if registry.get_contract(schema.dataset_name):
                logger.debug(
                    "Schema already registered",
                    schema=schema.dataset_name,
                    dataset=self.dataset_name,
                )
                continue
            registry.register_schema(schema)
            registered += 1

        self._rvu_bootstrapped = True
        logger.info(
            "RVU schemas bootstrap completed",
            dataset=self.dataset_name,
            registered=registered,
            total=len(schemas),
        )

    @staticmethod
    def get_contract(registry: Any, schema_name: str) -> Optional[Any]:
        """Best-effort lookup of a schema contract."""
        try:
            return registry.get_contract(schema_name)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Schema contract lookup failed", schema=schema_name, error=str(exc))
            return None

    @staticmethod
    def cache_schemas(registry: Any, dataset_to_schema: Dict[str, str]) -> Dict[str, Any]:
        """Populate an in-memory cache of schema contracts for validation."""
        cached: Dict[str, Any] = {}
        for dataset_name, schema_name in dataset_to_schema.items():
            contract = SchemaService.get_contract(registry, schema_name)
            if contract:
                cached[dataset_name] = contract
                logger.debug("Schema cached", dataset=dataset_name, schema=schema_name)
        return cached

    def _build_rvu_schema_contracts(self) -> List[SchemaContract]:
        """Create SchemaContract objects for all RVU datasets."""
        generated_at = datetime.utcnow().isoformat()

        def column(
            name: str,
            dtype: str,
            nullable: bool,
            description: str,
            **kwargs: Any,
        ) -> ColumnSpec:
            return ColumnSpec(
                name=name,
                type=dtype,
                nullable=nullable,
                description=description,
                unit=kwargs.get("unit"),
                domain=kwargs.get("domain"),
                min_value=kwargs.get("min_value"),
                max_value=kwargs.get("max_value"),
                pattern=kwargs.get("pattern"),
                sample_values=kwargs.get("sample_values"),
            )

        pprrvu_schema = SchemaContract(
            dataset_name="cms_pprrvu",
            version="1.0",
            generated_at=generated_at,
            columns={
                "hcpcs": column(
                    "hcpcs",
                    "str",
                    False,
                    "Healthcare Common Procedure Coding System code",
                    pattern=r"^[A-Z0-9]{5}$",
                ),
                "modifier": column(
                    "modifier",
                    "str",
                    True,
                    "HCPCS modifier code",
                    pattern=r"^[A-Z0-9]{2}$",
                ),
                "status_code": column(
                    "status_code",
                    "str",
                    False,
                    "Status code indicating if service is active",
                    domain=None,
                ),
                "global_days": column(
                    "global_days",
                    "str",
                    True,
                    "Global period days",
                    domain=["000", "010", "090", "XXX", "YYY", "ZZZ"],
                ),
                "rvu_work": column(
                    "rvu_work",
                    "float64",
                    True,
                    "Work RVU component",
                    min_value=0.0,
                    max_value=100.0,
                ),
                "rvu_pe_nonfac": column(
                    "rvu_pe_nonfac",
                    "float64",
                    True,
                    "Practice expense RVU (non-facility)",
                    min_value=0.0,
                    max_value=100.0,
                ),
                "rvu_pe_fac": column(
                    "rvu_pe_fac",
                    "float64",
                    True,
                    "Practice expense RVU (facility)",
                    min_value=0.0,
                    max_value=100.0,
                ),
                "rvu_malp": column(
                    "rvu_malp",
                    "float64",
                    True,
                    "Malpractice RVU component",
                    min_value=0.0,
                    max_value=10.0,
                ),
                "na_indicator": column(
                    "na_indicator",
                    "str",
                    True,
                    "Not applicable indicator",
                    domain=["Y", "N"],
                ),
                "opps_cap_applicable": column(
                    "opps_cap_applicable",
                    "bool",
                    True,
                    "Whether OPPS cap applies",
                ),
                "effective_from": column(
                    "effective_from",
                    "datetime64[ns]",
                    False,
                    "Effective start date",
                ),
                "effective_to": column(
                    "effective_to",
                    "datetime64[ns]",
                    True,
                    "Effective end date",
                ),
            },
            primary_keys=["hcpcs", "modifier", "effective_from"],
            partition_columns=["effective_from"],
            business_rules=[
                "HCPCS codes must be 5 characters",
                "Status code must be valid",
                "RVU components must be non-negative",
                "Global days must be valid if present",
            ],
            quality_thresholds={
                "null_rate_threshold": 0.01,
                "duplicate_rate_threshold": 0.0,
            },
        )

        gpci_schema = SchemaContract(
            dataset_name="cms_gpci",
            version="1.0",
            generated_at=generated_at,
            columns={
                "locality_code": column(
                    "locality_code",
                    "str",
                    False,
                    "2-digit locality code",
                    pattern=r"^\d{2}$",
                ),
                "state_fips": column(
                    "state_fips",
                    "str",
                    False,
                    "2-digit state FIPS code",
                    pattern=r"^\d{2}$",
                ),
                "gpci_work": column(
                    "gpci_work",
                    "float64",
                    False,
                    "Work GPCI index",
                    min_value=0.3,
                    max_value=2.0,
                ),
                "gpci_pe": column(
                    "gpci_pe",
                    "float64",
                    False,
                    "Practice expense GPCI index",
                    min_value=0.3,
                    max_value=2.0,
                ),
                "gpci_malp": column(
                    "gpci_malp",
                    "float64",
                    False,
                    "Malpractice GPCI index",
                    min_value=0.3,
                    max_value=2.0,
                ),
                "effective_from": column(
                    "effective_from",
                    "datetime64[ns]",
                    False,
                    "Effective start date",
                ),
                "effective_to": column(
                    "effective_to",
                    "datetime64[ns]",
                    True,
                    "Effective end date",
                ),
            },
            primary_keys=["locality_code", "state_fips", "effective_from"],
            partition_columns=["effective_from"],
            business_rules=[
                "Locality code must be 2 digits",
                "State FIPS must be 2 digits",
                "GPCI indices must be between 0.3 and 2.0",
            ],
            quality_thresholds={
                "null_rate_threshold": 0.0,
                "duplicate_rate_threshold": 0.0,
            },
        )

        oppscap_schema = SchemaContract(
            dataset_name="cms_oppscap",
            version="1.0",
            generated_at=generated_at,
            columns={
                "hcpcs": column(
                    "hcpcs",
                    "str",
                    False,
                    "HCPCS code",
                    pattern=r"^[A-Z0-9]{5}$",
                ),
                "modifier": column(
                    "modifier",
                    "str",
                    True,
                    "HCPCS modifier code",
                    pattern=r"^[A-Z0-9]{2}$",
                ),
                "opps_cap_applies": column(
                    "opps_cap_applies",
                    "bool",
                    False,
                    "Whether OPPS cap applies",
                ),
                "cap_amount_usd": column(
                    "cap_amount_usd",
                    "float64",
                    True,
                    "OPPS cap amount in USD",
                    min_value=0.0,
                ),
                "cap_method": column(
                    "cap_method",
                    "str",
                    True,
                    "Method used to calculate cap",
                    domain=["APC", "HCPCS", "CUSTOM"],
                ),
                "effective_from": column(
                    "effective_from",
                    "datetime64[ns]",
                    False,
                    "Effective start date",
                ),
                "effective_to": column(
                    "effective_to",
                    "datetime64[ns]",
                    True,
                    "Effective end date",
                ),
            },
            primary_keys=["hcpcs", "modifier", "effective_from"],
            partition_columns=["effective_from"],
            business_rules=[
                "HCPCS codes must be 5 characters",
                "Cap amount must be non-negative when cap applies",
                "Cap method must be valid if present",
            ],
            quality_thresholds={
                "null_rate_threshold": 0.05,
                "duplicate_rate_threshold": 0.0,
            },
        )

        anescf_schema = SchemaContract(
            dataset_name="cms_anescf",
            version="1.0",
            generated_at=generated_at,
            columns={
                "locality_code": column(
                    "locality_code",
                    "str",
                    False,
                    "2-digit locality code",
                    pattern=r"^\d{2}$",
                ),
                "state_fips": column(
                    "state_fips",
                    "str",
                    False,
                    "2-digit state FIPS code",
                    pattern=r"^\d{2}$",
                ),
                "anesthesia_cf_usd": column(
                    "anesthesia_cf_usd",
                    "float64",
                    False,
                    "Anesthesia conversion factor in USD",
                    min_value=0.0,
                    max_value=1000.0,
                ),
                "effective_from": column(
                    "effective_from",
                    "datetime64[ns]",
                    False,
                    "Effective start date",
                ),
                "effective_to": column(
                    "effective_to",
                    "datetime64[ns]",
                    True,
                    "Effective end date",
                ),
            },
            primary_keys=["locality_code", "state_fips", "effective_from"],
            partition_columns=["effective_from"],
            business_rules=[
                "Locality code must be 2 digits",
                "State FIPS must be 2 digits",
                "Conversion factor must be positive",
            ],
            quality_thresholds={
                "null_rate_threshold": 0.0,
                "duplicate_rate_threshold": 0.0,
            },
        )

        localitycounty_schema = SchemaContract(
            dataset_name="cms_localitycounty",
            version="1.0",
            generated_at=generated_at,
            columns={
                "mac": column(
                    "mac",
                    "str",
                    False,
                    "Medicare Administrative Contractor code",
                ),
                "locality_code": column(
                    "locality_code",
                    "str",
                    False,
                    "2-digit locality code",
                    pattern=r"^\d{2}$",
                ),
                "state_name": column(
                    "state_name",
                    "str",
                    True,
                    "State name (as published by CMS)",
                ),
                "fee_area": column(
                    "fee_area",
                    "str",
                    True,
                    "Fee schedule area description",
                ),
                "county_names": column(
                    "county_names",
                    "str",
                    True,
                    "Comma separated county names",
                ),
            },
            primary_keys=["mac", "locality_code"],
            partition_columns=[],
            business_rules=[
                "Locality code must be 2 digits",
                "MAC code must be present",
            ],
            quality_thresholds={
                "null_rate_threshold": 0.0,
                "duplicate_rate_threshold": 0.0,
            },
        )

        return [
            pprrvu_schema,
            gpci_schema,
            oppscap_schema,
            anescf_schema,
            localitycounty_schema,
        ]
