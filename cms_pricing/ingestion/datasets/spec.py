"""
DatasetSpec interface for plug-in dataset behavior.

Per DIS standards and PRD alignment:
- STD-parser-contracts-prd-v2.0.md: Schema contracts and parser versioning
- REF-parser-routing-detection-v1.0.md: File routing patterns
- STD-data-architecture-prd-v1.0.md: Natural keys and idempotency
"""

import re
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Any, Dict
from pandas import DataFrame

from ..contracts.ingestor_spec import ValidationRule
from ..validators.validation_engine import ValidationResult


@dataclass
class EnrichmentRule:
    """Enrichment rule for reference data joining"""
    name: str
    description: str
    source_column: str
    target_table: str
    join_keys: List[str]
    confidence_threshold: float = 0.8


@dataclass
class DatasetSpec:
    """
    Plugin interface for dataset-specific behavior per DIS standards.
    
    This class encapsulates all dataset-specific configuration:
    - Parser function reference (per STD-parser-contracts-prd-v2.0)
    - Schema contract ID (SemVer per §12.2)
    - Natural keys (per DIS §2)
    - Database loader function (idempotent upserts per DIS §7.2)
    - Validation rules (boolean/structural checks) and enrichment rules
    - Business rules (ValidationResult-based validators per dataset)
    - File routing patterns
    
    Specs are pure data structures (no business logic) to enable
    composition instead of switch statements.
    """
    dataset_id: str
    parser: Callable  # Parser function reference
    schema_id: str    # Schema contract ID (SemVer per STD-parser-contracts-prd-v2.0 §12.2)
    natural_keys: List[str]  # Natural key columns (per DIS §2)
    loader: Callable  # Database loader function (idempotent upserts per DIS §7.2)
    validation_rules: List[ValidationRule] = field(default_factory=list)
    enrichment_rules: List[EnrichmentRule] = field(default_factory=list)
    filename_patterns: List[str] = field(default_factory=list)  # For file discovery
    business_rules: List[Callable[[DataFrame], ValidationResult]] = field(default_factory=list)
    
    def route_file(self, filename: str, file_head: Optional[bytes] = None) -> bool:
        """
        Encapsulate file routing logic per REF-parser-routing-detection-v1.0.
        
        Matches filename patterns and optionally uses file_head for content sniffing.
        This method future-proofs routing for regex or metadata-based routing.
        
        Args:
            filename: Name of the file to route
            file_head: Optional first bytes for content sniffing (magic bytes, BOM)
            
        Returns:
            True if this spec matches the file, False otherwise
        """
        # Match filename patterns
        for pattern in self.filename_patterns:
            if re.match(pattern, filename, re.IGNORECASE):
                return True
        
        # Future: Add content sniffing using file_head if needed
        # This would check magic bytes, BOM, etc. per REF-parser-routing-detection-v1.0
        if file_head:
            # Example: Check for ZIP magic bytes
            if filename.lower().endswith('.zip') and file_head.startswith(b'PK'):
                # Could add more sophisticated content detection here
                pass
        
        return False
    
    def get_schema_version(self) -> str:
        """
        Extract schema version from schema_id (SemVer format).
        
        Returns:
            Version string (e.g., "1.0", "2.1") or "unknown"
        """
        # Schema IDs typically follow pattern: "cms_<dataset>_v<version>"
        # Example: "cms_pprrvu_v1.0" -> "1.0"
        match = re.search(r'v(\d+\.\d+)', self.schema_id)
        if match:
            return match.group(1)
        return "unknown"
